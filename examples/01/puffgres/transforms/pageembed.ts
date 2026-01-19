import type { RowEvent, Action, TransformContext, DocumentId } from 'puffgres';
import { getEncoding, type Tiktoken } from 'js-tiktoken';
import Together from 'together-ai';

// Cache the tokenizer instance
let tokenizer: Tiktoken | null = null;
let togetherClient: Together | null = null;

function getTokenizer(): Tiktoken {
  if (!tokenizer) {
    tokenizer = getEncoding('cl100k_base');
  }
  return tokenizer;
}

function getTogetherClient(apiKey: string): Together {
  if (!togetherClient) {
    togetherClient = new Together({ apiKey });
  }
  return togetherClient;
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
  const client = getTogetherClient(apiKey);
  const response = await client.embeddings.create({
    model: 'BAAI/bge-base-en-v1.5',
    input: text,
  });
  return response.data[0].embedding;
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
  const combinedText = [row.ocr_result].filter(Boolean).join(' ');

  // Tokenize and truncate to 512 tokens
  const truncatedText = truncateToTokens(combinedText, 512);

  // Use console.error for debug output
  console.error('truncatedText', truncatedText);

  // Embed using Together AI with BAAI/bge-base-en-v1.5
  const embedding = await embedWithTogether(truncatedText, ctx.env.TOGETHER_API_KEY);

  return {
    type: 'upsert',
    id,
    doc: {
      id: row.id,
      ocr_result: row.ocr_result,
      vector: embedding,
    },
    distance_metric: 'cosine_distance',
  };
}