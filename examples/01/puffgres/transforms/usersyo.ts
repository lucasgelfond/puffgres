import type { TransformInput, Action, TransformContext, DocumentId } from 'puffgres';
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

async function embedBatchWithTogether(texts: string[], apiKey: string): Promise<number[][]> {
  if (texts.length === 0) return [];

  const client = getTogetherClient(apiKey);
  const response = await client.embeddings.create({
    model: 'BAAI/bge-base-en-v1.5',
    input: texts,
  });

  // Sort by index to ensure correct ordering
  return response.data
    .sort((a, b) => a.index - b.index)
    .map(d => d.embedding);
}

export default async function transform(
  rows: TransformInput[],
  ctx: TransformContext
): Promise<Action[]> {
  // Separate deletes from upserts
  const deleteActions: { index: number; action: Action }[] = [];
  const upsertRows: { index: number; id: DocumentId; row: Record<string, unknown>; text: string }[] = [];

  for (let i = 0; i < rows.length; i++) {
    const { event, id } = rows[i];

    if (event.op === 'delete') {
      deleteActions.push({ index: i, action: { type: 'delete', id } });
      continue;
    }

    const row = event.new!;
    const combinedText = [row.name, row.email].filter(Boolean).join(' ');
    const truncatedText = truncateToTokens(combinedText, 500);

    upsertRows.push({ index: i, id, row, text: truncatedText });
  }

  // Batch embed all texts at once
  const textsToEmbed = upsertRows.map(r => r.text);
  console.error(`Embedding ${textsToEmbed.length} texts in single batch`);

  const embeddings = await embedBatchWithTogether(textsToEmbed, ctx.env.TOGETHER_API_KEY);

  // Build upsert actions with embeddings
  const upsertActions = upsertRows.map((r, i) => ({
    index: r.index,
    action: {
      type: 'upsert' as const,
      id: r.id,
      doc: {
        id: r.row.id,
        name: r.row.name,
        email: r.row.email,
        vector: embeddings[i],
      },
      distance_metric: 'cosine_distance' as const,
    },
  }));

  // Merge and sort by original index to preserve order
  const allActions = [...deleteActions, ...upsertActions]
    .sort((a, b) => a.index - b.index)
    .map(a => a.action);

  return allActions;
}
