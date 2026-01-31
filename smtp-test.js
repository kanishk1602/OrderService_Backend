require('dotenv').config();
const nodemailer = require('nodemailer');

async function testSMTP() {
  const transporter = nodemailer.createTransport({
    host: process.env.SMTP_HOST,
    port: process.env.SMTP_PORT,
    secure: false,
    auth: {
      user: process.env.SMTP_USER,
      pass: process.env.SMTP_PASS,
    },
  });

  try {
    const info = await transporter.sendMail({
      from: process.env.SMTP_FROM || process.env.SMTP_USER,
      to: process.env.SMTP_USER, // send test mail to yourself
      subject: 'SMTP Test from order-service',
      text: 'This is a test email from your Node.js order-service SMTP setup.'
    });
    console.log('Test email sent:', info.messageId);
  } catch (err) {
    console.error('SMTP test failed:', err);
  }
}

testSMTP();
