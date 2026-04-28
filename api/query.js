import { BigQuery } from '@google-cloud/bigquery';

export default async function handler(req, res) {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  if (req.method === 'OPTIONS') return res.status(204).end();
  if (req.method !== 'POST') return res.status(405).json({ error: 'Método não permitido.' });

  const { ids } = req.body || {};
  if (!ids || ids.length === 0) {
    return res.status(400).json({ error: 'Parâmetro "ids" é obrigatório.' });
  }

  // Lê a service account da variável de ambiente
  let credentials;
  try {
    credentials = JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_KEY);
  } catch {
    return res.status(500).json({ error: 'Variável GOOGLE_SERVICE_ACCOUNT_KEY inválida ou não configurada.' });
  }

  const bigquery = new BigQuery({
    projectId: 'bidata-cross-sa-batch',
    credentials,
  });

  // Sanitiza os IDs para evitar SQL injection
  const safeIds = ids
    .map(id => id.trim().replace(/[^a-zA-Z0-9_]/g, ''))
    .filter(Boolean);

  if (safeIds.length === 0) {
    return res.status(400).json({ error: 'Nenhum ID válido.' });
  }

  const idsLiteral = safeIds.map(id => `'${id}'`).join(', ');

  const query = `
    WITH filters AS (
      SELECT
        id AS original_id,
        SAFE_CAST(SPLIT(id, '_')[OFFSET(0)] AS INT64) AS clean_id
      FROM UNNEST([${idsLiteral}]) AS id
    ),
    agg AS (
      SELECT
        f.original_id AS SHP_SENDER_ID,
        i.ITE_ITEM_ID AS item_id,
        ANY_VALUE(i.SHP_ITEM_DESC) AS item_desc,
        SUM(SAFE_CAST(i.SHP_QUANTITY AS INT64)) AS qty_sold,
        MAX(SAFE_CAST(i.SHP_ITE_DIMENSION_MAX AS FLOAT64)) AS shp_ite_dimension_max,
        MAX(SAFE_CAST(i.SHP_ITE_DIMENSION_MID AS FLOAT64)) AS shp_ite_dimension_mid,
        MAX(SAFE_CAST(i.SHP_ITE_DIMENSION_MIN AS FLOAT64)) AS shp_ite_dimension_min
      FROM \`meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS\` s
      CROSS JOIN UNNEST(s.ITEMS) AS i
      INNER JOIN filters f ON s.SHP_SENDER_ID = f.clean_id
      WHERE i.SHP_CREATED_DT BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
      GROUP BY 1, 2
    )
    SELECT *
    FROM agg
    QUALIFY DENSE_RANK() OVER (
      PARTITION BY SHP_SENDER_ID
      ORDER BY qty_sold DESC
    ) <= 20
    ORDER BY SHP_SENDER_ID, qty_sold DESC
  `;

  try {
    const [rows] = await bigquery.query({ query });
    return res.status(200).json({ data: rows });
  } catch (err) {
    console.error('Erro BigQuery:', err);
    return res.status(500).json({ error: 'Erro ao consultar o BigQuery.', details: err.message });
  }
}
