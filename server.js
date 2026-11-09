require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WsClient = require('ws');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Railway / Uptime Health Check endpoint
app.get('/', (req, res) => {
  res.json({
    status: 'running',
    message: 'Dhan WebSocket Gateway is active',
    activeSubscribers: activeSubscriptions.size,
    timestamp: new Date().toISOString(),
  });
});

// ==========================================
// Credentials from Environment Variables
// ==========================================
const CLIENT_ID = process.env.DHAN_CLIENT_ID;
const ACCESS_TOKEN = process.env.DHAN_ACCESS_TOKEN;

if (!CLIENT_ID || !ACCESS_TOKEN) {
  console.error('⚠️ [CRITICAL] DHAN_CLIENT_ID or DHAN_ACCESS_TOKEN is missing in environment variables!');
}

// ==========================================
// REST Route: Historical OHLC Data Proxy
// ==========================================
app.get('/api/charts/historical', async (req, res) => {
  try {
    const {
      securityId,
      exchangeSegment = 'NSE_EQ',
      instrument = 'EQUITY',
      fromDate,
      toDate,
    } = req.query;

    if (!securityId) {
      return res.status(400).json({ error: 'securityId parameter is required' });
    }

    if (!ACCESS_TOKEN) {
      return res.status(500).json({ error: 'Dhan ACCESS_TOKEN is not configured on server' });
    }

    // Default dates: past 30 days to today (YYYY-MM-DD)
    const today = new Date().toISOString().split('T')[0];
    const past = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

    const payload = {
      securityId: String(securityId),
      exchangeSegment: exchangeSegment,
      instrument: instrument,
      expiryCode: 0,
      oi: false,
      fromDate: fromDate || past,
      toDate: toDate || today,
    };

    const dhanRes = await fetch('https://api.dhan.co/v2/charts/historical', {
      method: 'POST',
      headers: {
        'access-token': ACCESS_TOKEN,
        'client-id': CLIENT_ID || '',
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      body: JSON.stringify(payload),
    });

    const data = await dhanRes.json();

    // Dhan returns timestamp or start_Time array
    const timestamps = data.timestamp || data.start_Time;
    if (data && data.open && data.open.length > 0 && timestamps) {
      const bars = timestamps.map((time, idx) => {
        const epochSeconds = typeof time === 'number'
          ? (time > 9999999999 ? Math.floor(time / 1000) : time)
          : Math.floor(new Date(time).getTime() / 1000);

        return {
          time: epochSeconds,
          open: Number(data.open[idx]),
          high: Number(data.high[idx]),
          low: Number(data.low[idx]),
          close: Number(data.close[idx]),
          volume: Number(data.volume ? data.volume[idx] : 0),
        };
      });

      // Sort chronological ascending order for Lightweight Charts
      bars.sort((a, b) => a.time - b.time);
      return res.json({ success: true, count: bars.length, bars });
    }

    return res.status(404).json({
      error: 'No candle data returned from Dhan',
      details: data,
    });
  } catch (error) {
    console.error('[Historical API Error]', error.message);
    res.status(500).json({ error: error.message });
  }
});

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
  },
});

const DHAN_WS_URL = `wss://api-feed.dhan.co?version=2&token=${encodeURIComponent(
  ACCESS_TOKEN || ''
)}&clientId=${encodeURIComponent(CLIENT_ID || '')}&authType=2`;

let dhanWs = null;
const activeSubscriptions = new Map(); // securityId -> subscriber count

// ------------------------------------------
// 1. Dhan WebSocket Manager
// ------------------------------------------
function connectDhan() {
  if (!CLIENT_ID || !ACCESS_TOKEN) return;

  console.log('[Dhan] Connecting to Live Feed...');
  dhanWs = new WsClient(DHAN_WS_URL, {
    headers: { 'User-Agent': 'DhanHQ-NodeClient/2.0' },
  });

  dhanWs.on('open', () => {
    console.log('[Dhan] Connected successfully.');

    // Handshake packet (RequestCode 11)
    dhanWs.send(
      JSON.stringify({
        RequestCode: 11,
        ClientId: CLIENT_ID,
        Token: ACCESS_TOKEN,
      })
    );

    // अगर रीकनेक्ट हुआ है, तो पहले से एक्टिव सिंबल्स दोबारा सब्सक्राइब करें
    if (activeSubscriptions.size > 0) {
      const instruments = Array.from(activeSubscriptions.keys()).map((id) => ({
        ExchangeSegment: 'NSE_EQ',
        SecurityId: id.toString(),
      }));
      sendSubscription(15, instruments);
    }
  });

  dhanWs.on('message', (data) => {
    // Ack / Handshake JSON strings
    if (typeof data === 'string' || data[0] === 0x7b) return;

    // Binary Tick Data Parsing
    const buffer = Buffer.from(data);
    const responseCode = buffer.readUInt8(0);

    if (responseCode === 2 || responseCode === 4 || responseCode === 8) {
      const exchangeSegment = buffer.readUInt8(1);
      const securityId = buffer.readInt32LE(4);
      const ltp = buffer.readFloatLE(8);

      const tick = {
        securityId: securityId.toString(),
        segment: exchangeSegment,
        ltp: Number(ltp.toFixed(2)),
        timestamp: Date.now(),
      };

      // संबंधित रूम को टिक ब्रॉडकास्ट करें
      io.to(`stock:${securityId}`).emit('tick', tick);
    }
  });

  dhanWs.on('error', (err) => {
    console.error('[Dhan Error]', err.message);
  });

  dhanWs.on('close', (code, reason) => {
    console.log(`[Dhan] Disconnected (Code: ${code}). Reconnecting in 3 seconds...`);
    setTimeout(connectDhan, 3000);
  });
}

function sendSubscription(requestCode, instrumentList) {
  if (dhanWs && dhanWs.readyState === WsClient.OPEN && instrumentList.length > 0) {
    dhanWs.send(
      JSON.stringify({
        RequestCode: requestCode, // 15 = Subscribe, 16 = Unsubscribe
        InstrumentCount: instrumentList.length,
        InstrumentList: instrumentList,
      })
    );
  }
}

connectDhan();

// ------------------------------------------
// 2. Client (Lovable / Browser) Manager
// ------------------------------------------
io.on('connection', (socket) => {
  console.log(`[Client Connected] Socket ID: ${socket.id}`);

  // यूजर ने स्टॉक वॉचलिस्ट में खोला / चार्ट लोड किया
  socket.on('subscribe', ({ securityId, segment = 'NSE_EQ' }) => {
    if (!securityId) return;

    const room = `stock:${securityId}`;
    socket.join(room);

    const currentCount = activeSubscriptions.get(securityId) || 0;
    activeSubscriptions.set(securityId, currentCount + 1);

    // केवल तभी सब्सक्राइब भेजें जब पहला यूजर इस स्टॉक को देख रहा हो
    if (currentCount === 0) {
      sendSubscription(15, [{ ExchangeSegment: segment, SecurityId: securityId.toString() }]);
      console.log(`[Subscribed Dhan] SecurityId: ${securityId}`);
    }
  });

  // यूजर ने स्टॉक बदला या टैब बंद किया
  socket.on('unsubscribe', ({ securityId, segment = 'NSE_EQ' }) => {
    if (!securityId) return;

    const room = `stock:${securityId}`;
    socket.leave(room);

    const currentCount = activeSubscriptions.get(securityId) || 0;
    if (currentCount <= 1) {
      activeSubscriptions.delete(securityId);
      sendSubscription(16, [{ ExchangeSegment: segment, SecurityId: securityId.toString() }]);
      console.log(`[Unsubscribed Dhan] SecurityId: ${securityId}`);
    } else {
      activeSubscriptions.set(securityId, currentCount - 1);
    }
  });

  socket.on('disconnect', () => {
    console.log(`[Client Disconnected] Socket ID: ${socket.id}`);
  });
});

// Railway PORT binding on 0.0.0.0
const PORT = process.env.PORT || 4000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`🚀 Gateway Server running on port ${PORT}`);
});
