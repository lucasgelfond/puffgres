examples/01/puffgres/transforms/user_vector.ts
@@ -1,78 +0,0 @@
import type { RowEvent, Action, TransformContext, DocumentId } from 'puffgres';
import { getEncoding, type Tiktoken } from 'js-tiktoken';

// Cache the tokenizer instance
let tokenizer: Tiktoken | null = null;

function getTokenizer(): Tiktoken {
  if (!tokenizer) {
    tokenizer = getEncoding('cl100k_base');
  }
  return tokenizer;
}

function truncateToTokens(text: string, maxTokens: number): string {
  const tok = getTokenizer();
  const tokenIds = tok.encode(text);
  if (tokenIds.length <= maxTokens) {
    return text;
  }
  const truncatedIds = tokenIds.slice(0, maxTokens);
  return tok.decode(truncatedIds);
}

async function embedWithTogether(text: string, apiKey: string): Promise<number[]> {
  const response = await fetch('https://api.together.xyz/v1/embeddings', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`,
    },
    body: JSON.stringify({
      model: 'BAAI/bge-base-en-v1.5',
      input: text,
    }),
  });

  if (!response.ok) {
    const error = await response.text();
    throw new Error(`Together AI error: ${error}`);
  }

  const data = await response.json() as { data: Array<{ embedding: number[] }> };
  return data.data[0].embedding;
}

export default async function transform(
  event: RowEvent,
  id: DocumentId,
  ctx: TransformContext
): Promise<Action> {
  if (event.op === 'delete') {
    return { type: 'delete', id };
  }

  const row = event.new!;

  // Combine name and email
  const combinedText = [row.name, row.email].filter(Boolean).join(' ');

  // Tokenize and truncate to 512 tokens
  const truncatedText = truncateToTokens(combinedText, 512);

  console.log('truncatedText', truncatedText);

  // Embed using Together AI with BAAI/bge-base-en-v1.5
  const embedding = await embedWithTogether(truncatedText, ctx.env.TOGETHER_API_KEY);

  return {
    type: 'upsert',
    id,
    doc: {
      id: row.id,
      email: row.email,
      name: row.name,
      vector: embedding,
    },
  };
}