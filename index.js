import debug from 'debug'
import pipe from 'it-pipe'
import { Cluster } from '@nftstorage/ipfs-cluster'
import pg from 'pg'
import { getCandidate } from './candidates.js'

const log = debug('repin:index')

/**
 * @param {Object} config
 * @param {Date} [config.startDate] Date to consider for searching from.
 * @param {string} config.dbConnString PostgreSQL connection string.
 * @param {string} config.clusterApiUrl
 * @param {string} config.clusterAuthToken
 */
export async function startRepinUnpinned ({
  startDate,
  dbConnString,
  clusterApiUrl,
  clusterAuthToken
}) {
  log(`starting from ${startDate}...`)

  log('connecting to PostgreSQL...')
  const db = new pg.Client({ connectionString: dbConnString })
  await db.connect()
  log('connected to PostgreSQL')
  log('creating cluster client')
  const cluster = new Cluster(clusterApiUrl, {
    headers: clusterAuthToken ? { Authorization: `Basic ${clusterAuthToken}` } : {}
  })

  try {
    await pipe(getCandidate(db, startDate), async (source) => {
      // TODO: parallelise
      for await (const candidate of source) {
        log(`processing candidate ${candidate.cid}`)
        try {
          log(`recover ${candidate.cid}`)
          await cluster.recover(candidate.cid.toString())
        } catch (err) {
          log(`failed to repin ${candidate}`, err)
        }
      }
    })
  } finally {
    try {
      log('closing DB connection...')
      await db.end()
    } catch (err) {
      log('failed to close DB connection:', err)
    }
  }
}
