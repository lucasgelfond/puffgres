import type { RowEvent, Action, TransformContext, DocumentId } from 'puffgres';

export default async function transform(
  event: RowEvent,
  id: DocumentId,
  ctx: TransformContext
): Promise<Action> {
  // ctx.migration contains: { name, namespace, table }

  if (event.op === 'delete') {
    return { type: 'delete', id };
  }

  const row = event.new!;

  return {
    type: 'upsert',
    id,
    doc: {
      id: row.id,
      email: row.email,
      name: row.name,
    },
  };
}
