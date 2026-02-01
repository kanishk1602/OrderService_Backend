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
const allowedOrigins = (process.env.CORS_ORIGINS || "https://microservices-ecom.vercel.app,http://localhost:3000,http://localhost:3001")
  .split(",")
  .map((o) => o.trim())
  .filter(Boolean);

const corsOptions = {
  origin: allowedOrigins,
  credentials: true,
  methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
};

app.use(cors(corsOptions));
// Explicitly handle preflight requests for all routes
app.options("*", cors(corsOptions));

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

// Kafka configuration - supports both local and cloud (Upstash/Confluent)
const kafkaBrokers = process.env.KAFKA_BROKERS 
  ? process.env.KAFKA_BROKERS.split(',').map(b => b.trim())
  : ["localhost:9094"];

const kafkaConfig = {
  clientId: "order-service",
  brokers: kafkaBrokers,
};

// Add SASL authentication if credentials are provided (for Upstash/Confluent)
if (process.env.KAFKA_USE_SASL === 'true' && process.env.KAFKA_USERNAME && process.env.KAFKA_PASSWORD) {
  kafkaConfig.sasl = {
    mechanism: 'plain',
    username: process.env.KAFKA_USERNAME,
    password: process.env.KAFKA_PASSWORD,
  };
  kafkaConfig.ssl = true; // Upstash/Confluent require SSL
}

const kafka = new Kafka(kafkaConfig);

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
            
            // Determine payment provider first
            const provider = payload.gateway || (paymentId || razorpayOrderId ? 'razorpay' : (payload.paymentIntentId ? 'stripe' : 'unknown'));
            
            // Use appropriate order ID based on provider
            const oid = provider === 'razorpay' ? razorpayOrderId : 
                       provider === 'stripe' ? (payload.sessionId || payload.paymentIntentId) :
                       `order_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;

            const providerOrderId = provider === 'razorpay' ? razorpayOrderId : (payload.paymentIntentId || payload.sessionId || null);
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
               const paymentMethodLabel = provider.charAt(0).toUpperCase() + provider.slice(1);
               const paymentIdentifier = provider === 'razorpay' ? (paymentId || providerPaymentId) : (providerPaymentId || providerOrderId);

               // Build HTML email
               const itemsHtml = (cart || []).map((item) => `
                 <tr>
                   <td style="padding: 12px; border-bottom: 1px solid #eee;">${item.name}</td>
                   <td style="padding: 12px; border-bottom: 1px solid #eee; text-align: center;">${item.quantity}</td>
                   <td style="padding: 12px; border-bottom: 1px solid #eee; text-align: right;">₹${item.price?.toFixed(2) || '0.00'}</td>
                   <td style="padding: 12px; border-bottom: 1px solid #eee; text-align: right;">₹${((item.price || 0) * (item.quantity || 0)).toFixed(2)}</td>
                 </tr>
               `).join('');

               const emailHtml = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 0; background-color: #f5f5f5;">
  <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff;">
    <!-- Header -->
    <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); padding: 30px; text-align: center;">
      <h1 style="color: #ffffff; margin: 0; font-size: 28px;">Order Confirmed! ✓</h1>
      <p style="color: rgba(255,255,255,0.9); margin: 10px 0 0 0;">Thank you for your purchase</p>
    </div>
    
    <!-- Order Info -->
    <div style="padding: 30px;">
      <div style="background-color: #f8f9fa; border-radius: 8px; padding: 20px; margin-bottom: 25px;">
        <table style="width: 100%; border-collapse: collapse;">
          <tr>
            <td style="padding: 5px 0;"><strong>Order ID:</strong></td>
            <td style="padding: 5px 0; text-align: right; color: #667eea;">${oid}</td>
          </tr>
          <tr>
            <td style="padding: 5px 0;"><strong>Date:</strong></td>
            <td style="padding: 5px 0; text-align: right;">${new Date().toLocaleDateString('en-IN', { year: 'numeric', month: 'long', day: 'numeric' })}</td>
          </tr>
          <tr>
            <td style="padding: 5px 0;"><strong>Payment Method:</strong></td>
            <td style="padding: 5px 0; text-align: right;">${paymentMethodLabel}</td>
          </tr>
          <tr>
            <td style="padding: 5px 0;"><strong>Status:</strong></td>
            <td style="padding: 5px 0; text-align: right;"><span style="background-color: #28a745; color: white; padding: 3px 10px; border-radius: 12px; font-size: 12px;">PAID</span></td>
          </tr>
        </table>
      </div>
      
      <!-- Order Items -->
      <h2 style="color: #333; font-size: 18px; margin-bottom: 15px; border-bottom: 2px solid #667eea; padding-bottom: 10px;">Order Details</h2>
      <table style="width: 100%; border-collapse: collapse; margin-bottom: 25px;">
        <thead>
          <tr style="background-color: #f8f9fa;">
            <th style="padding: 12px; text-align: left; font-weight: 600;">Item</th>
            <th style="padding: 12px; text-align: center; font-weight: 600;">Qty</th>
            <th style="padding: 12px; text-align: right; font-weight: 600;">Price</th>
            <th style="padding: 12px; text-align: right; font-weight: 600;">Total</th>
          </tr>
        </thead>
        <tbody>
          ${itemsHtml}
        </tbody>
      </table>
      
      <!-- Totals -->
      <div style="background-color: #f8f9fa; border-radius: 8px; padding: 20px;">
        <table style="width: 100%; border-collapse: collapse;">
          <tr>
            <td style="padding: 8px 0;">Subtotal:</td>
            <td style="padding: 8px 0; text-align: right;">₹${total.toFixed(2)}</td>
          </tr>
          <tr>
            <td style="padding: 8px 0;">Shipping:</td>
            <td style="padding: 8px 0; text-align: right; color: #28a745;">FREE</td>
          </tr>
          <tr style="border-top: 2px solid #667eea;">
            <td style="padding: 12px 0; font-size: 18px;"><strong>Total:</strong></td>
            <td style="padding: 12px 0; text-align: right; font-size: 18px; color: #667eea;"><strong>₹${total.toFixed(2)}</strong></td>
          </tr>
        </table>
      </div>
      
      <!-- Payment Reference -->
      <p style="color: #666; font-size: 12px; margin-top: 20px; text-align: center;">
        Payment Reference: ${paymentIdentifier || 'N/A'}
      </p>
    </div>
    
    <!-- Footer -->
    <div style="background-color: #f8f9fa; padding: 20px; text-align: center; border-top: 1px solid #eee;">
      <p style="color: #666; margin: 0 0 10px 0; font-size: 14px;">Thank you for shopping with us!</p>
      <p style="color: #999; margin: 0; font-size: 12px;">If you have any questions, please contact our support team.</p>
    </div>
  </div>
</body>
</html>
               `.trim();

               // Plain text fallback
               const emailText = `Order Confirmation - ${oid}\n\nThank you for your order!\n\nOrder ID: ${oid}\nDate: ${new Date().toLocaleString()}\nPayment: ${paymentMethodLabel}\nStatus: PAID\n\nTotal: ₹${total.toFixed(2)}\n\nPayment Reference: ${paymentIdentifier || 'N/A'}`;

              await producer.send({
                topic: "send-email",
                messages: [
                  {
                    value: JSON.stringify({
                      to: email,
                      subject: `Order Confirmation - ${oid}`,
                      text: emailText,
                      html: emailHtml,
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
              const emailHtml = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 0; background-color: #f5f5f5;">
  <div style="max-width: 600px; margin: 0 auto; background-color: #ffffff;">
    <!-- Header -->
    <div style="background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%); padding: 30px; text-align: center;">
      <h1 style="color: #ffffff; margin: 0; font-size: 28px;">Payment Failed ✗</h1>
      <p style="color: rgba(255,255,255,0.9); margin: 10px 0 0 0;">We couldn't process your payment</p>
    </div>
    
    <!-- Content -->
    <div style="padding: 30px;">
      <div style="background-color: #fff3f3; border-left: 4px solid #e74c3c; padding: 20px; margin-bottom: 25px;">
        <p style="margin: 0; color: #333;">We're sorry, but your payment attempt was unsuccessful.</p>
      </div>
      
      <div style="background-color: #f8f9fa; border-radius: 8px; padding: 20px; margin-bottom: 25px;">
        <table style="width: 100%; border-collapse: collapse;">
          <tr>
            <td style="padding: 8px 0;"><strong>Order ID:</strong></td>
            <td style="padding: 8px 0; text-align: right;">${oid}</td>
          </tr>
          <tr>
            <td style="padding: 8px 0;"><strong>Attempted on:</strong></td>
            <td style="padding: 8px 0; text-align: right;">${new Date().toLocaleDateString('en-IN', { year: 'numeric', month: 'long', day: 'numeric', hour: '2-digit', minute: '2-digit' })}</td>
          </tr>
          <tr>
            <td style="padding: 8px 0;"><strong>Status:</strong></td>
            <td style="padding: 8px 0; text-align: right;"><span style="background-color: #e74c3c; color: white; padding: 3px 10px; border-radius: 12px; font-size: 12px;">FAILED</span></td>
          </tr>
        </table>
      </div>
      
      <h2 style="color: #333; font-size: 18px; margin-bottom: 15px;">What to do next:</h2>
      <ul style="color: #666; line-height: 1.8; padding-left: 20px;">
        <li>Check your payment method and try again</li>
        <li>Verify your card has sufficient funds</li>
        <li>Contact your bank if the issue persists</li>
        <li>Reach out to our support team for assistance</li>
      </ul>
      
      <p style="color: #28a745; margin-top: 20px; padding: 15px; background-color: #f0fff4; border-radius: 8px; text-align: center;">
        ✓ Your cart items are still saved and ready for checkout.
      </p>
    </div>
    
    <!-- Footer -->
    <div style="background-color: #f8f9fa; padding: 20px; text-align: center; border-top: 1px solid #eee;">
      <p style="color: #666; margin: 0 0 10px 0; font-size: 14px;">Thank you for your patience.</p>
      <p style="color: #999; margin: 0; font-size: 12px;">If you need help, please contact our support team.</p>
    </div>
  </div>
</body>
</html>
              `.trim();

              const emailText = `Payment Failed - Order ${oid}\n\nWe're sorry, but your payment attempt was unsuccessful.\n\nOrder ID: ${oid}\nAttempted on: ${new Date().toLocaleString()}\nStatus: FAILED\n\nPlease check your payment method and try again.\n\nYour cart items are still saved.`;

              await producer.send({
                topic: "send-email",
                messages: [
                  {
                    value: JSON.stringify({
                      to: email,
                      subject: `Payment Failed - Order ${oid}`,
                      text: emailText,
                      html: emailHtml,
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
