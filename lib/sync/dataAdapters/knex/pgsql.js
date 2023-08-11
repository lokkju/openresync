const _ = require('lodash')

const {
  getPrimaryKeyField,
  getTimestampFields,
  shouldIncludeField,
  getFieldNamesThatShouldBeIncluded,
} = require('../../utils')
const knex = require("knex");


module.exports = (destinationConfig) => {

  const db = knex({
    client: 'pg',
    connection: destinationConfig.connectionString,
    // debug: true,
    pool: {
      // Putting min:0 fixes the idle timeout message of:
      // "Connection Error: Error: Connection lost: The server closed the connection."
      // See: https://stackoverflow.com/a/55858656/135101
      min: 0,
    },
  })

  function setPlatformAdapter() {
    // Do nothing
  }

  function setPlatformDataAdapter() {
    // Do nothing
  }

  async function syncStructure() {
    // Do nothing
  }

  async function syncData() {
    // Do nothing
  }

  async function getTimestamps(mlsResourceName, indexes) {
    const updateTimestampFields = _.pickBy(indexes, v => v.isUpdateTimestamp)
    return _.mapValues(updateTimestampFields, () => new Date(0))
  }

  async function getAllIds() {
    return []
  }

  async function getCount() {
    return 0
  }

  async function getMostRecentTimestamp(mlsResourceName) {
    return null
  }

  async function purge() {
    // Do nothing
  }

  async function closeConnection() {
    // Do nothing
  }

  return {
    syncStructure,
    syncData,
    getTimestamps,
    closeConnection,
    setPlatformAdapter,
    setPlatformDataAdapter,
    getAllIds,
    purge,
    getCount,
  }
}
