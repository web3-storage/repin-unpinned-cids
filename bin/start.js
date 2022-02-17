#!/usr/bin/env node

import dotenv from 'dotenv'
import { startRepinUnpinned } from '../index.js'

dotenv.config()

startRepinUnpinned({
  startDate: process.argv[2] ? new Date(process.argv[2]) : undefined,
  dbConnString: mustGetEnv('DATABASE_CONNECTION'),
  clusterApiUrl: mustGetEnv('CLUSTER_API_URL'),
  clusterAuthToken: mustGetEnv('CLUSTER_AUTH_TOKEN')
})

/**
 * @param {string} name
 */
function mustGetEnv(name) {
  const value = process.env[name]
  if (!value) throw new Error(`missing ${name} environment variable`)
  return value
}
