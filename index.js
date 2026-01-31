import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";
import dotenv from "dotenv";
import mongoose from "mongoose";
import jwt from "jsonwebtoken";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 4001;

// Middleware
app.use(
  cors({
    origin: ["http://localhost:3000", "http://localhost:3001"],
    credentials: true,
  })
);
app.use(express.json());

// --- MongoDB setup ---
const MONGODB_URL = process.env.MONGODB_URL;
if (!MONGODB_URL) {
  console.warn("MONGODB_URL not set. Set it to enable persistence.");
}
mongoose
  .connect(MONGODB_URL || "mongodb://localhost:27017/microshop", {
    dbName: process.env.MONGODB_DB || "microshop",
  })
  .then(() => console.log("Order Service: MongoDB connected"))
  .catch((err) => console.error("Mongo connection error", err));

// --- Models ---
const CartSchema = new mongoose.Schema(
  {
    userId: { type: String, index: true, unique: true, required: true },
    items: [
      {
        productId: String,
        name: String,
        image: String,
        price: Number,
        quantity: Number,
      },
    ],
    updatedAt: { type: Date, default: Date.now },
  },
  { timestamps: true }
);
const OrderSchema = new mongoose.Schema(
  {
    orderId: { type: String, index: true, unique: true, required: true },
    userId: { type: String, index: true, required: true },
    email: { type: String, index: true },
    items: [
      {
        productId: String,
        name: String,
        image: String,
        price: Number,
        quantity: Number,
      },
    ],
    total: Number,
    status: { type: String, enum: ["pending", "paid", "failed"], default: "pending" },
    payment: {
      provider: String,
      providerOrderId: String,
      paymentId: String,
      signature: String,
    },
  },
  { timestamps: true }
);
const Cart = mongoose.models.Cart || mongoose.model("Cart", CartSchema);
const Order = mongoose.models.Order || mongoose.model("Order", OrderSchema);

// --- Auth middleware ---
const AUTH_JWT_SECRET = process.env.AUTH_JWT_SECRET || process.env.JWT_SECRET;
console.log('Order Service JWT Secret configured:', AUTH_JWT_SECRET ? 'YES' : 'NO');
function authenticate(req, res, next) {
  const authHeader = req.headers.authorization || "";
  const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7) : null;
  if (!token) {
    console.log('Auth failed: No token provided');
    return res.status(401).json({ error: "Unauthorized - No token" });
  }
  try {
    const payload = jwt.verify(token, AUTH_JWT_SECRET);
    console.log('Token verified, payload:', { id: payload.id, email: payload.email, role: payload.role });
    // Expect payload to contain id and email from auth-service
    req.user = { id: payload.id || payload._id || payload.userId, email: payload.email };
    if (!req.user.id) {
      console.log('Auth failed: No user ID in token');
      return res.status(401).json({ error: "Invalid token - No user ID" });
    }
    next();
  } catch (e) {
    console.log('Auth failed: JWT verification error:', e.message);
    return res.status(401).json({ error: "Invalid token - " + e.message });
  }
}

// Kafka setup
const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9094"],
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: "order-service" });

// Health check endpoint
app.get("/health", (req, res) => {
  res.json({
    status: "OK",
    service: "Order Service",
    port: PORT,
    timestamp: new Date().toISOString(),
  });
});

// Get service info
app.get("/", (req, res) => {
  res.json({
    service: "Order Service",
    version: "1.0.0",
    port: PORT,
    endpoints: {
      health: "/health",
      createOrder: "/api/orders (POST)",
      getOrders: "/api/orders (GET)",
      cartGet: "/api/cart (GET)",
      cartPut: "/api/cart (PUT)",
      cartDelete: "/api/cart (DELETE)",
    },
  });
});

// --- Cart endpoints ---
app.get("/api/cart", authenticate, async (req, res) => {
  try {
    const doc = await Cart.findOne({ userId: req.user.id }).lean();
    res.json({ success: true, cart: doc || { userId: req.user.id, items: [] } });
  } catch (e) {
    res.status(500).json({ success: false, error: "Failed to get cart" });
  }
});

app.put("/api/cart", authenticate, async (req, res) => {
  try {
    const { items } = req.body;
    if (!Array.isArray(items)) return res.status(400).json({ error: "items must be array" });
    const doc = await Cart.findOneAndUpdate(
      { userId: req.user.id },
      { items, updatedAt: new Date() },
      { upsert: true, new: true }
    ).lean();
    res.json({ success: true, cart: doc });
  } catch (e) {
    res.status(500).json({ success: false, error: "Failed to save cart" });
  }
});

app.delete("/api/cart", authenticate, async (req, res) => {
  try {
    await Cart.deleteOne({ userId: req.user.id });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ success: false, error: "Failed to clear cart" });
  }
});

// --- Orders endpoints ---
app.get("/api/orders", authenticate, async (req, res) => {
  try {
    const orders = await Order.find({ userId: req.user.id }).sort({ createdAt: -1 }).lean();
    res.json({ success: true, orders });
  } catch (e) {
    res.status(500).json({ success: false, error: "Failed to get orders" });
  }
});

app.post("/api/orders", authenticate, async (req, res) => {
  try {
    const { items, total, orderId } = req.body;
    if (!Array.isArray(items) || items.length === 0) {
      return res.status(400).json({ error: "Items array is required" });
    }
    const oid = orderId || `order_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
    const order = await Order.create({
      orderId: oid,
      userId: req.user.id,
      email: req.user.email,
      items,
      total,
      status: "pending",
    });
    res.status(201).json({ success: true, order });
  } catch (e) {
    res.status(500).json({ success: false, error: "Failed to create order" });
  }
});

// --- Existing mock GET retained for compatibility ---
app.get("/api/orders/mock", async (req, res) => {
  try {
    // For now, return a mock response
    // In a real application, we would query a database
    res.json({
      success: true,
      orders: [
        {
          orderId: "mock_order_1",
          status: "completed",
          total: 299.99,
          createdAt: new Date().toISOString(),
        },
      ],
      message: "Orders retrieved successfully",
    });
  } catch (error) {
    console.error("Error getting orders:", error);
    res.status(500).json({
      success: false,
      error: "Failed to get orders",
    });
  }
});

const run = async () => {
  try {
    // Connect to Kafka
    await producer.connect();
    await consumer.connect();

    // --- Kafka consumers ---
    await consumer.subscribe({ topic: "payment-successful", fromBeginning: true });
    await consumer.subscribe({ topic: "payment-failed", fromBeginning: true });

    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();
        let payload;
        try {
          payload = JSON.parse(value);
        } catch (e) {
          console.error("Invalid message", e);
          return;
        }
        const { cart, email, userId, razorpayOrderId, paymentId, signature } = payload;

        if (topic === "payment-successful") {
          try {
            // Debug: Log incoming payload
            console.log('[Order] Received payment-successful:', {
              gateway: payload.gateway,
              email: email,
              userId: userId,
              cartItems: cart?.length || 0,
            });

            // Create or update order as paid
            const total = Array.isArray(cart) ? cart.reduce((s, i) => s + i.price * i.quantity, 0) : 0;
            const oid = razorpayOrderId || `order_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;

            // Determine payment provider and ids
            const provider = payload.gateway || (paymentId || razorpayOrderId ? 'razorpay' : (payload.paymentIntentId ? 'stripe' : 'unknown'));
            const providerOrderId = provider === 'razorpay' ? razorpayOrderId : (payload.paymentIntentId || null);
            const providerPaymentId = provider === 'razorpay' ? paymentId : (payload.paymentIntentId || null);
            const providerSignature = provider === 'razorpay' ? signature : (payload.signature || null);

            await Order.updateOne(
              { orderId: oid },
              {
                orderId: oid,
                userId: userId || "anonymous",
                email: email || "",
                items: (cart || []).map((i) => ({
                  productId: String(i.id || i.productId || ""),
                  name: i.name,
                  image: i.image,
                  price: i.price,
                  quantity: i.quantity,
                })),
                total,
                status: "paid",
                // Save provider info dynamically
                payment: {
                  provider,
                  providerOrderId: providerOrderId || undefined,
                  paymentId: providerPaymentId || undefined,
                  signature: providerSignature || undefined,
                },
               },
               { upsert: true }
             );
             console.log(`Order ${oid} marked as paid in database`);

             // Clear cart for user
             if (userId && userId !== 'anonymous') {
               await Cart.deleteOne({ userId });
               console.log(`Cart cleared for user ${userId}`);
             }

             // Publish email event to Kafka (email-service will handle sending)
             if (email) {
               // Format order details for email
               const itemsList = (cart || []).map((item, idx) => 
                 `${idx + 1}. ${item.name} - Qty: ${item.quantity} × ₹${item.price?.toFixed(2) || '0.00'} = ₹${((item.price || 0) * (item.quantity || 0)).toFixed(2)}`
               ).join('\n');

              // Build payment lines dynamically for email
              const paymentMethodLabel = provider.charAt(0).toUpperCase() + provider.slice(1);
              const paymentIdentifier = provider === 'razorpay' ? (paymentId || providerPaymentId) : (providerPaymentId || providerOrderId);

              const emailText = `
Order Confirmation
==================

Thank you for your order!

Order Details:
--------------
${itemsList || 'No items'}

Subtotal: ₹${total.toFixed(2)}
Tax: ₹0.00
Shipping: FREE
--------------
Total: ₹${total.toFixed(2)}

Payment Method: ${paymentMethodLabel}

Thank you for shopping with us!

If you have any questions, please contact support.
Payment Reference: ${paymentIdentifier || 'N/A'}

Order ID: ${oid}
Order Date: ${new Date().toLocaleString()}
Payment Status: PAID
              `.trim();

              await producer.send({
                topic: "send-email",
                messages: [
                  {
                    value: JSON.stringify({
                      to: email,
                      subject: `Order Confirmation - ${oid}`,
                      text: emailText,
                      type: "order-success",
                      orderId: oid,
                      total,
                    }),
                  },
                ],
              });
              console.log(`Email event published for ${email} with order details`);
             }
           } catch (e) {
             console.error("Failed to persist paid order", e);
           }
        } else if (topic === "payment-failed") {
          try {
            const oid = razorpayOrderId || `order_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
            await Order.updateOne(
              { orderId: oid },
              { status: "failed", payment: { provider: "razorpay", providerOrderId: razorpayOrderId, paymentId, signature } },
              { upsert: true }
            );
            console.log(`Order ${oid} marked as failed in database`);

            // Publish email event for payment failure
            if (email) {
              const emailText = `
Payment Failed
==============

We're sorry, but your payment attempt was unsuccessful.

Order ID: ${oid}
Attempted on: ${new Date().toLocaleString()}
Status: FAILED

What to do next:
- Please check your payment method and try again
- Verify your card has sufficient funds
- Contact your bank if the issue persists
- Reach out to our support team for assistance

Your cart items are still saved and ready for checkout.

Thank you for your patience.
              `.trim();

              await producer.send({
                topic: "send-email",
                messages: [
                  {
                    value: JSON.stringify({
                      to: email,
                      subject: `Payment Failed - Order ${oid}`,
                      text: emailText,
                      type: "payment-failed",
                      orderId: oid,
                    }),
                  },
                ],
              });
              console.log(`Payment failure email event published for ${email}`);
            }
          } catch (e) {
            console.error("Failed to persist failed payment", e);
          }
        }
      },
    });

    // Start Express server
    app.listen(PORT, () => {
      console.log(`🚀 Order Service running on port ${PORT}`);
      console.log(`📊 Health check: http://localhost:${PORT}/health`);
      console.log(`📋 API docs: http://localhost:${PORT}/`);
    });
  } catch (err) {
    console.error("Error starting order service:", err);
    process.exit(1);
  }
};

run();
