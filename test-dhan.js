const WsClient = require('ws');

// ==========================================
// अपनी Dhan क्रेडेंशियल्स यहाँ भरें
// ==========================================
const CLIENT_ID = '1110828273';   // जैसे: '1100123456'
const ACCESS_TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJ1c2VyUmVnaW9uIjoiUjEiLCJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzk0NTQzMjk0LCJpYXQiOjE3OTQ0NTY4OTQsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTEwODI4MjczIn0.f-ZZ9F1lFaA5dVnFZTf_ZV1D4iDRpodBD0uwd3h53V-I5fvShZnt5kglmXiCzsZbAqiIYU4lYEfjICe7zqU3OQ'; // आपका 24 घंटे वाला Access Token

// Dhan v2 WebSocket URL with Query Parameters (यही मिसिंग था)
const DHAN_WS_URL = `wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(ACCESS_TOKEN)}&clientId=${encodeURIComponent(CLIENT_ID)}&authType=2`;

console.log('Connecting to Dhan v2 Live Feed WebSocket...');

const ws = new WsClient(DHAN_WS_URL, {
  headers: {
    'User-Agent': 'DhanHQ-NodeClient/2.0',
  },
});

ws.on('open', () => {
  console.log('WebSocket Connection Opened Successfully!');

  // 1. Authorize Packet (RequestCode 11)
  const authPayload = {
    RequestCode: 11,
    ClientId: CLIENT_ID,
    Token: ACCESS_TOKEN,
  };

  ws.send(JSON.stringify(authPayload));
  console.log('Sent Authentication Handshake (Code 11)...');

  // 2. Subscribe Instruments (RequestCode 15)
  // Instrument types: 15 = Ticker (LTP), 17 = Quote, 21 = Full
  setTimeout(() => {
    if (ws.readyState === WsClient.OPEN) {
      const subscriptionPayload = {
        RequestCode: 15, // 15 = Ticker Mode
        InstrumentCount: 2,
        InstrumentList: [
          {
            ExchangeSegment: 'NSE_EQ',
            SecurityId: '1333', // HDFC Bank (जैसा Dhan doc में दिया है)
          },
          {
            ExchangeSegment: 'NSE_EQ',
            SecurityId: '2885', // Reliance Industries
          },
        ],
      };

      ws.send(JSON.stringify(subscriptionPayload));
      console.log('Subscribed to HDFC Bank (1333) & Reliance (2885)...');
      console.log('Waiting for live market ticks...\n');
    }
  }, 1000);
});

ws.on('message', (data) => {
  // अगर टेक्स्ट/JSON मैसेज आया (Server Handshake Ack / Status)
  if (typeof data === 'string' || data[0] === 0x7b) {
    try {
      console.log('Server Message:', JSON.parse(data.toString()));
      return;
    } catch (e) {}
  }

  // Dhan Binary Packet Parsing
  const buffer = Buffer.from(data);
  const responseCode = buffer.readUInt8(0);

  // 2 = Ticker Packet (LTP), 4 = Quote, 8 = Depth
  if (responseCode === 2 || responseCode === 4 || responseCode === 8) {
    const exchangeSegment = buffer.readUInt8(1);
    const securityId = buffer.readInt32LE(4);
    const ltp = buffer.readFloatLE(8);

    console.log(
      `[TICK] ${new Date().toLocaleTimeString()} | SecurityId: ${securityId} | Segment: ${exchangeSegment} | LTP: ₹${ltp.toFixed(2)}`
    );
  } else {
    console.log(`Packet received | Code: ${responseCode} | Length: ${buffer.length} bytes`);
  }
});

ws.on('error', (error) => {
  console.error('WebSocket Error:', error.message);
});

ws.on('close', (code, reason) => {
  console.log(`Connection closed. Code: ${code}, Reason: ${reason ? reason.toString() : 'None'}`);
});
