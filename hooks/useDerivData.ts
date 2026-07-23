import { useEffect, useState } from 'react';
import { derivWS, VolatilityPair, TickData } from '@/lib/deriv-websocket';

export function useDerivTicks(symbol: VolatilityPair) {
  const [ticks, setTicks] = useState<TickData[]>([]);

  useEffect(() => {
    const handleTick = (tick: TickData) => {
      setTicks(prev => [tick, ...prev].slice(0, 100)); // Keep last 100 ticks
    };

    derivWS.subscribeToTicks(symbol, handleTick);

    return () => {
      derivWS.unsubscribeFromTicks(symbol, handleTick);
    };
  }, [symbol]);

  return ticks;
}
