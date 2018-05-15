/********************************************************************
 * Descricao: Função que envia os dados de telemetria para o BigQuery
 * Data: 2018/05/15
 * Versao 1.0
 ********************************************************************/

const functions = require('firebase-functions');
const admin = require('firebase-admin');
const bigquery = require('@google-cloud/bigquery')();
const cors = require('cors')({ origin: true });


/**
 * Recebe os dados brutos do Pub/sub e os 
 * envia para a BigQuery no formato JSON.
 */
exports.receiveTelemetry = functions.pubsub
  .topic('telemetry-topic').onPublish(event => {
    const message = event.data.json;

    return Promise.all([
      insertIntoBigquery(message),
    ]);
  });


/**
 * Salva os dados na Tabela do BigQuery
 */
function insertIntoBigquery(data) {
  const dataset = bigquery.dataset("BigQueryRaspberry");
  const table = dataset.table("bigquery_sensor_data");

  return table.insert(data);
}
