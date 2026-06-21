import { useEffect, useState } from 'react';

const TYPE_DELAY_MS = 75;
const DELETE_DELAY_MS = 40;
const PAUSE_AFTER_TYPE_MS = 2200;

export function useTypewriterPlaceholder(phrases: string[]): string {
  const [phraseIndex, setPhraseIndex] = useState(0);
  const [displayText, setDisplayText] = useState('');
  const [isDeleting, setIsDeleting] = useState(false);

  const safePhrases = phrases.length > 0 ? phrases : [''];
  const currentPhrase = safePhrases[phraseIndex % safePhrases.length] ?? '';

  useEffect(() => {
    let timeoutId: ReturnType<typeof setTimeout>;

    if (!isDeleting && displayText === currentPhrase) {
      timeoutId = setTimeout(() => setIsDeleting(true), PAUSE_AFTER_TYPE_MS);
    } else if (isDeleting && displayText.length === 0) {
      setIsDeleting(false);
      setPhraseIndex((prev) => (prev + 1) % safePhrases.length);
    } else {
      const delay = isDeleting ? DELETE_DELAY_MS : TYPE_DELAY_MS;
      timeoutId = setTimeout(() => {
        setDisplayText((prev) =>
          isDeleting
            ? prev.slice(0, -1)
            : currentPhrase.slice(0, prev.length + 1),
        );
      }, delay);
    }

    return () => clearTimeout(timeoutId);
  }, [currentPhrase, displayText, isDeleting, safePhrases.length]);

  return displayText;
}
