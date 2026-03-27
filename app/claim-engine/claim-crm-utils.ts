/** Heuristic 0–100 score from the latest marketplace/agent text. */
export function estimateClaimSuccessProbability(lastMessage: string): number {
  const m = lastMessage.toLowerCase();
  if (!m.trim()) return 50;
  if (/\b(approved|accept|accepted|granted|reimbursed|paid)\b/.test(m)) return 92;
  if (/\b(denied|denial|reject|declined|not eligible|closed.*deny)\b/.test(m)) return 12;
  if (/\b(evidence|upload|document|additional information|more detail|provide proof)\b/.test(m)) return 44;
  if (/\b(pending|under review|processing|investigating)\b/.test(m)) return 58;
  return 52;
}
