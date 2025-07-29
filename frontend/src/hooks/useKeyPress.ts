import { useEffect, useState, useMemo } from 'react';

export function useKeyPress(targetKeys: string | string[]): boolean {
  const [keyPressed, setKeyPressed] = useState(false);
  
  // Memoize the keys array to prevent dependency changes
  const keys = useMemo(() => {
    return Array.isArray(targetKeys) ? targetKeys : [targetKeys];
  }, [targetKeys]);

  useEffect(() => {
    const downHandler = (event: KeyboardEvent) => {
      // Don't intercept events from input/textarea elements
      if (event.target instanceof HTMLInputElement || 
          event.target instanceof HTMLTextAreaElement ||
          (event.target as HTMLElement).contentEditable === 'true') {
        return;
      }
      
      if (keys.includes(event.key)) {
        event.preventDefault();
        setKeyPressed(true);
      }
    };

    const upHandler = (event: KeyboardEvent) => {
      // Don't intercept events from input/textarea elements
      if (event.target instanceof HTMLInputElement || 
          event.target instanceof HTMLTextAreaElement ||
          (event.target as HTMLElement).contentEditable === 'true') {
        return;
      }
      
      if (keys.includes(event.key)) {
        event.preventDefault();
        setKeyPressed(false);
      }
    };

    window.addEventListener('keydown', downHandler);
    window.addEventListener('keyup', upHandler);

    return () => {
      window.removeEventListener('keydown', downHandler);
      window.removeEventListener('keyup', upHandler);
    };
  }, [keys]);

  return keyPressed;
}