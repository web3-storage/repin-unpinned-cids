import debug from 'debug'
import { CID } from 'multiformats'
import formatNumber from 'format-number'

const fmt = formatNumber()
const log = debug('repin:candidate')

const COUNT_PIN_CANDIDATES = `
   SELECT COUNT(*) FROM (
	   SELECT DISTINCT content_cid FROM pin p
	   WHERE p.inserted_at > $1
	   AND p.status='Unpinned'
   ) AS unpinnedCount
`

const GET_PIN_CANDIDATES = `
    SELECT
      DISTINCT ON (content_cid) content_cid
    FROM pin p
    WHERE p.inserted_at > $1
    AND p.status='Unpinned'
    ORDER BY content_cid
    OFFSET $2
    LIMIT $3
`

async function countPins(db, startDate) {
  log('counting unpinned pinns...')
  const { rows } = await db.query(COUNT_PIN_CANDIDATES, [startDate.toISOString()])
  log(`found ${fmt(rows[0].count)} unpinned pins backup`)
  return rows[0].count
}

/**
 * Fetch a list of CIDs that need to be repinned.
 *
 * @param {import('pg').Client} db Postgres client.
 * @param {Date} [startDate]
 */
export async function* getCandidate(db, startDate = new Date(0)) {
  console.log('get total')
  const totalCandidates = await countPins(db, startDate)
  console.log('total candidates:', totalCandidates)
  let offset = 0
  const limit = 1000
  let total = 0
  while (true) {
    log(`fetching ${fmt(limit)} pins since ${startDate.toISOString()}...`)
    const { rows: pins } = await db.query(GET_PIN_CANDIDATES, [
      startDate.toISOString(),
      offset,
      limit
    ])
    if (!pins.length) break

    for (const pin of pins) {
      log(`processing ${fmt(total + 1)} of ${fmt(totalCandidates)}`)
      yield CID.parse(pin.content_cid)
      total++
    }

    offset += limit
  }
}
