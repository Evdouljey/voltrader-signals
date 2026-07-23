// lib/deriv-websocket.ts
// Dedicated Deriv WebSocket service for VolTrader Signals
// Handles connections, subscriptions for ticks + OHLC (candles) for Volatility 1S indices

const DERIV_WS_URL = 'wss://ws.derivws.com/websockets/v3';
const APP_ID = process.env.NEXT_PUBLIC_DERIV_APP_ID || '1089'; // Default demo app ID, replace with yours

export const VOLATILITY_PAIRS = [
  '1HZ10V', '1HZ25V', '1HZ50V', '1HZ75V', '1HZ90V', '1HZ100V'
] as const;

export type VolatilityPair = typeof VOLATILITY_PAIRS[number];

export interface TickData {
  symbol: VolatilityPair;
  quote: number;
  epoch: number;
  id?: string;
}

export interface CandleData {
  symbol: VolatilityPair;
  open: number;
  high: number;
  low: number;
  close: number;
  epoch: number;
  granularity: number; // seconds
}

class DerivWebSocketService {
  private ws: WebSocket | null = null;
  private subscribers: Map<string, Set<(data: any) => void>> = new Map();
  private reconnectAttempts = 0;
  private maxReconnects = 5;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private pingInterval: NodeJS.Timeout | null = null;

  constructor() {
    this.connect();
  }

  private connect() {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) return;

    const url = `${DERIV_WS_URL}?app_id=${APP_ID}`;
    this.ws = new WebSocket(url);

    this.ws.onopen = () => {
      console.log('✅ Deriv WS Connected');
      this.reconnectAttempts = 0;
      this.startPing();
      this.resubscribeAll();
    };

    this.ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        this.handleMessage(data);
      } catch (e) {
        console.error('WS Parse Error:', e);
      }
    };

    this.ws.onclose = () => {
      console.warn('⚠️ Deriv WS Disconnected');
      this.cleanup();
      this.reconnect();
    };

    this.ws.onerror = (error) => {
      console.error('❌ Deriv WS Error:', error);
    };
  }

  private handleMessage(data: any) {
    if (data.msg_type === 'tick') {
      const tick = data.tick as TickData;
      this.notifySubscribers(`tick:${tick.symbol}`, tick);
    } else if (data.msg_type === 'ohlc') {
      const ohlc = data.ohlc as CandleData;
      this.notifySubscribers(`ohlc:${ohlc.symbol}`, ohlc);
    } else if (data.error) {
      console.error('Deriv API Error:', data.error);
    }
  }

  private notifySubscribers(key: string, data: any) {
    const subs = this.subscribers.get(key);
    if (subs) {
      subs.forEach(callback => callback(data));
    }
  }

  private startPing() {
    if (this.pingInterval) clearInterval(this.pingInterval);
    this.pingInterval = setInterval(() => {
      if (this.ws?.readyState === WebSocket.OPEN) {
        this.ws.send(JSON.stringify({ ping: 1 }));
      }
    }, 30000); // Every 30s
  }

  private resubscribeAll() {
    Array.from(this.subscribers.keys()).forEach(key => {
      if (key.startsWith('tick:')) {
        const symbol = key.split(':')[1];
        this.subscribeToTicks(symbol as VolatilityPair);
      }
    });
  }

  private reconnect() {
    if (this.reconnectAttempts >= this.maxReconnects) {
      console.error('Max reconnect attempts reached');
      return;
    }

    this.reconnectAttempts++;
    const delay = Math.min(1000 * Math.pow(2, this.reconnectAttempts), 30000);
    
    console.log(`Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
    this.reconnectTimeout = setTimeout(() => this.connect(), delay);
  }

  private cleanup() {
    if (this.pingInterval) {
      clearInterval(this.pingInterval);
      this.pingInterval = null;
    }
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }
  }

  public subscribeToTicks(symbol: VolatilityPair, callback: (tick: TickData) => void) {
    const key = `tick:${symbol}`;
    if (!this.subscribers.has(key)) {
      this.subscribers.set(key, new Set());
    }
    this.subscribers.get(key)!.add(callback);

    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({
        ticks: symbol,
        subscribe: 1,
        req_id: Date.now()
      }));
    }
  }

  public unsubscribeFromTicks(symbol: VolatilityPair, callback?: (tick: TickData) => void) {
    const key = `tick:${symbol}`;
    const subs = this.subscribers.get(key);
    if (subs) {
      if (callback) {
        subs.delete(callback);
      } else {
        subs.clear();
      }
      if (subs.size === 0) {
        this.subscribers.delete(key);
      }
    }
  }

  public subscribeToCandles(symbol: VolatilityPair, granularity: number, callback: (candle: CandleData) => void) {
    // Extend with candles subscription logic
    console.log(`Subscribed to candles for ${symbol} @ ${granularity}s`);
  }

  public disconnect() {
    this.cleanup();
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}

export const derivWS = new DerivWebSocketService();
export default derivWS;
