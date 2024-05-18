/* eslint-disable camelcase */
/* eslint-disable @typescript-eslint/no-unused-vars */
import "./env";
import fetch from "isomorphic-fetch";
import { ethers } from "ethers";
import * as zksync from "zksync";
import fs from "fs";
import moment from "moment";
import * as Sentry from "@sentry/node";
import { bold, fmt, link } from "telegraf/format";

import { concatFmt, launchTgBot, notifyReferrerNewRef, notifyUser } from "./tg";
import { redis, publisher } from "./redisClient";
import db from "./db";
import {
  formatPrice,
  getNetwork,
  getRPCURL,
  getFeeEstimationMarket,
  getReadableTxError,
  sortMarketPair,
  CMC_IDS
} from "./utils";
import type {
  ZZMarketInfo,
  AnyObject,
  ZZMarket,
  ZZMarketSummary
} from "./types";

Sentry.init({
  dsn: process.env.SENTRY_DSN
})

const NUMBER_OF_SNAPSHOT_POSITIONS = 200

const VALID_CHAINS: number[] = process.env.VALID_CHAINS ? JSON.parse(process.env.VALID_CHAINS) : [1, 1002, 1001, 42161, 421613]
const VALID_CHAINS_ZKSYNC: number[] = VALID_CHAINS.filter((chainId) => [1, 1002].includes(chainId))
const VALID_EVM_CHAINS: number[] = VALID_CHAINS.filter((chainId) => [42161, 421613].includes(chainId))
const ZKSYNC_BASE_URL: AnyObject = {}
const SYNC_PROVIDER: AnyObject = {}
const ETHERS_PROVIDERS: AnyObject = {}
const EXCHANGE_CONTRACTS: AnyObject = {}
let EVMConfig: AnyObject = {}
let ERC20_ABI: any

const updatePendingOrdersDelay = 5

async function getMarketInfo(market: ZZMarket, chainId: number) {
  if (!VALID_CHAINS.includes(chainId) || !market) return null

  const redisKeyMarketInfo = `marketinfo:${chainId}`
  const cache = await redis.HGET(redisKeyMarketInfo, market)
  if (cache) return JSON.parse(cache) as ZZMarketInfo

  return null
}

async function updatePendingOrders() {
  console.time('updatePendingOrders')

  const oneMinAgo = new Date(Date.now() - 60 * 1000).toISOString()
  let orderUpdates: string[][] = []
  const query = {
    text: "UPDATE offers SET order_status='c', update_timestamp=NOW() WHERE (order_status IN ('m', 'b', 'pm') AND update_timestamp < $1) OR (order_status='o' AND unfilled <= 0) RETURNING chainid, id, order_status;",
    values: [oneMinAgo],
  }
  const update = await db.query(query)
  if (update.rowCount > 0) {
    orderUpdates = orderUpdates.concat(update.rows.map((row) => [row.chainid, row.id, row.order_status]))
  }

  // Update fills
  const fillsQuery = {
    text: "UPDATE fills SET fill_status='e', feeamount=0 WHERE fill_status IN ('m', 'b', 'pm') AND insert_timestamp < $1",
    values: [oneMinAgo],
  }
  await db.query(fillsQuery)

  const expiredTimestamp = ((Date.now() / 1000) | 0) + Math.floor(updatePendingOrdersDelay)
  const expiredQuery = {
    text: "UPDATE offers SET order_status='e', update_timestamp=NOW() WHERE order_status IN ('o', 'pm', 'pf') AND expires < $1 RETURNING chainid, id, order_status, zktx, side, market",
    values: [expiredTimestamp],
  }
  const updateExpires = await db.query(expiredQuery)
  if (updateExpires.rowCount > 0) {
    orderUpdates = orderUpdates.concat(updateExpires.rows.map((row) => [row.chainid, row.id, row.order_status]))
    for (let i = 0; i < updateExpires.rows.length; i++) {
      const expiredOrder = updateExpires.rows[i];
      // eslint-disable-next-line no-continue
      if (!expiredOrder.zktx) continue;
      const zktx = JSON.parse(expiredOrder.zktx);
      const {id, side, market} = expiredOrder;
      notifyUser(concatFmt(
        fmt`⌛️❌ Order expired #${id} `,
        bold`${side === 'b' ? 'BUY 🟢 ' : `SELL 🔴 `}`,
        link(market, `https://trade.zklite.io/?market=${market}&network=zksync`)
      ), {
        chainId: 1, address: zktx.recipient,
        link_preview_options: {is_disabled: true}
      })
    }
  }

  if (orderUpdates.length > 0) {
    VALID_CHAINS.forEach((chainId: number) => {
      const updatesForThisChain = orderUpdates.filter((row) => Number(row[0]) === chainId)
      publisher.PUBLISH(`broadcastmsg:all:${chainId}:all`, JSON.stringify({ op: 'orderstatus', args: [updatesForThisChain] }))
    })
  }
  console.timeEnd('updatePendingOrders')
}

async function updateUsdPrice() {
  console.time('Updating usd price.')

  // use mainnet as price source TODO we should rework the price source to work with multible networks
  const network = getNetwork(1)
  const results0: Promise<any>[] = VALID_CHAINS.map(async (chainId) => {
    const updatedTokenPrice: any = {}
    // fetch redis
    const markets = await redis.SMEMBERS(`activemarkets:${chainId}`)
    const tokenInfos = await redis.HGETALL(`tokeninfo:${chainId}`)
    // get active tokens once
    const tokenSymbols = Array.from(new Set(markets.join('-').split('-')))
    const fetchResult = (await fetch(
        `https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?symbol=${tokenSymbols.join(',')}`, {
      headers: {
        'X-CMC_PRO_API_KEY': process.env.CMC_API_KEY
      }
    }).then((r: any) => r.json()).catch((e: any) => {
      console.error('Fetch token usd price failed', e)
    })) as AnyObject

    if (!fetchResult) return

    const results1: Promise<any>[] = tokenSymbols.map(async (token: string) => {
      const tokenInfoString = tokenInfos[token]
      if (!tokenInfoString) return
      const tokenInfo = JSON.parse(tokenInfoString)
      const cmcId = CMC_IDS[token]
      if  (!cmcId) {
        console.error(`Unknown CMC id for token ${token}`)
        return
      }
      try {
        const usdPrice = fetchResult.data[token.toUpperCase()]?.find((it: any) => it.id === cmcId)?.quote?.USD?.price
        if (!Number.isFinite(usdPrice) || usdPrice <= 0) {
          console.error(`Can't find CMC quote for ${token}`)
          return
        }

        updatedTokenPrice[token] = usdPrice
        tokenInfo.usdPrice = usdPrice
      } catch (err: any) {
        console.log(`Could not update price for ${token}, Error: ${err.message}`)
      }
      await redis.HSET(`tokeninfo:${chainId}`, tokenInfo.symbol, JSON.stringify(tokenInfo))
      // redis.HSET(`tokeninfo:${chainId}`, tokenInfo.address, JSON.stringify(tokenInfo))
    })
    await Promise.all(results1)

    const marketInfos = await redis.HGETALL(`marketinfo:${chainId}`)
    const results2: Promise<any>[] = markets.map(async (market: ZZMarket) => {
      if (!marketInfos[market]) return
      const marketInfo = JSON.parse(marketInfos[market])
      if (updatedTokenPrice[marketInfo.baseAsset.symbol]) {
        marketInfo.baseAsset.usdPrice = Number(formatPrice(updatedTokenPrice[marketInfo.baseAsset.symbol]))
      }
      if (updatedTokenPrice[marketInfo.quoteAsset.symbol]) {
        marketInfo.quoteAsset.usdPrice = Number(formatPrice(updatedTokenPrice[marketInfo.quoteAsset.symbol]))
      }
      await redis.HSET(`marketinfo:${chainId}`, market, JSON.stringify(marketInfo))
      await publisher.PUBLISH(`broadcastmsg:all:${chainId}:${market}`, JSON.stringify({ op: 'marketinfo', args: [marketInfo] }))
    })
    await Promise.all(results2)
  })
  await Promise.all(results0)
  console.timeEnd('Updating usd price.')
}

async function updateFeesZkSync() {
  console.time('Update fees zkSync')

  const results0: Promise<any>[] = VALID_CHAINS_ZKSYNC.map(async (chainId: number) => {
    const newFees: any = {}
    const network = getNetwork(chainId)
    // get redis cache
    const tokenInfos: any = await redis.HGETALL(`tokeninfo:${chainId}`)
    const markets = await redis.SMEMBERS(`activemarkets:${chainId}`)
    // get every token form activemarkets once
    let tokenSymbols = markets.join('-').split('-')
    tokenSymbols = tokenSymbols.filter((x, i) => i === tokenSymbols.indexOf(x))
    // update fee for each
    const results1: Promise<any>[] = tokenSymbols.map(async (tokenSymbol: string) => {
      let fee = 0
      const tokenInfoString = tokenInfos[tokenSymbol]
      if (!tokenInfoString) return

      const tokenInfo = JSON.parse(tokenInfoString)
      if (!tokenInfo) return
      // enabledForFees -> get fee dircectly form zkSync
      if (tokenInfo.enabledForFees) {
        try {
          const feeReturn = await SYNC_PROVIDER[network].getTransactionFee(
            'Swap',
            '0x88d23a44d07f86b2342b4b06bd88b1ea313b6976',
            tokenSymbol
          )
          fee = Number(SYNC_PROVIDER[network].tokenSet.formatToken(tokenSymbol, feeReturn.totalFee))
        } catch (e: any) {
          console.log(`Can't get fee for ${tokenSymbol}, error: ${e.message}`)
        }
      }
      // not enabledForFees -> use token price and USDC fee
      if (!fee) {
        try {
          const usdPrice: number = tokenInfo.usdPrice ? Number(tokenInfo.usdPrice) : 0
          const usdReferenceString = await redis.HGET(`tokenfee:${chainId}`, 'USDC')
          const usdReference: number = usdReferenceString ? Number(usdReferenceString) : 0
          if (usdPrice > 0) {
            fee = usdReference / usdPrice
          }
        } catch (e) {
          console.log(`Can't get fee per reference for ${tokenSymbol}, error: ${e}`)
        }
      }

      // save new fee
      newFees[tokenSymbol] = fee
      if (fee) {
        redis.HSET(`tokenfee:${chainId}`, tokenSymbol, fee)
      }
    })
    await Promise.all(results1)

    // check if fee's have changed
    const marketInfos = await redis.HGETALL(`marketinfo:${chainId}`)
    const results2: Promise<any>[] = markets.map(async (market: ZZMarket) => {
      if (!marketInfos[market]) return
      const marketInfo = JSON.parse(marketInfos[market])
      const newBaseFee = newFees[marketInfo.baseAsset.symbol]
      const newQuoteFee = newFees[marketInfo.quoteAsset.symbol]
      let updated = false
      if (newBaseFee && marketInfo.baseFee !== newBaseFee) {
        marketInfo.baseFee = Number(newFees[marketInfo.baseAsset.symbol]) * 1.05
        updated = true
      }
      if (newQuoteFee && marketInfo.quoteFee !== newQuoteFee) {
        marketInfo.quoteFee = Number(newFees[marketInfo.quoteAsset.symbol]) * 1.05
        updated = true
      }
      if (updated) {
        redis.HSET(`marketinfo:${chainId}`, market, JSON.stringify(marketInfo))
        publisher.PUBLISH(`broadcastmsg:all:${chainId}:${market}`, JSON.stringify({ op: 'marketinfo', args: [marketInfo] }))
      }
    })
    await Promise.all(results2)
  })
  await Promise.all(results0)
  console.timeEnd('Update fees zkSync')
}

// Removes old liquidity
// Updates lastprice redis map
// Sets best bids and asks in a JSON for broadcasting
async function removeOldLiquidity() {
  console.time('removeOldLiquidity')

  const results0: Promise<any>[] = VALID_CHAINS_ZKSYNC.map(async (chainId) => {
    const markets = await redis.SMEMBERS(`activemarkets:${chainId}`)
    const results1: Promise<any>[] = markets.map(async (marketId) => {
      const redisKeyLiquidity = `liquidity2:${chainId}:${marketId}`
      const liquidityList = await redis.HGETALL(redisKeyLiquidity)
      const liquidity = []
      // eslint-disable-next-line no-restricted-syntax, guard-for-in
      for (const clientId in liquidityList) {
        const liquidityPosition = JSON.parse(liquidityList[clientId])
        liquidity.push(...liquidityPosition)
      }

      // remove from activemarkets if no liquidity exists
      if (liquidity.length === 0) {
        // redis.SREM(`activemarkets:${chainId}`, marketId)
        return
      }

      const uniqueAsk: any = {}
      const uniqueBuy: any = {}
      for (let i = 0; i < liquidity.length; i++) {
        const entry = liquidity[i]
        const price = Number(entry[1])
        const amount = Number(entry[2])

        // merge positions in object
        if (entry[0] === 'b') {
          uniqueBuy[price] = uniqueBuy[price] ? uniqueBuy[price] + amount : amount
        } else {
          uniqueAsk[price] = uniqueAsk[price] ? uniqueAsk[price] + amount : amount
        }
      }

      // sort ask and bid keys
      const askSet = [...new Set(Object.keys(uniqueAsk))]
      const bidSet = [...new Set(Object.keys(uniqueBuy))]
      const lenghtAsks = askSet.length < NUMBER_OF_SNAPSHOT_POSITIONS ? askSet.length : NUMBER_OF_SNAPSHOT_POSITIONS
      const lengthBids = bidSet.length < NUMBER_OF_SNAPSHOT_POSITIONS ? bidSet.length : NUMBER_OF_SNAPSHOT_POSITIONS
      const asks = new Array(lenghtAsks)
      const bids = new Array(lengthBids)

      // Update last price
      for (let i = 0; i < lenghtAsks; i++) {
        asks[i] = ['s', Number(askSet[i]), Number(uniqueAsk[askSet[i]])]
      }
      for (let i = 0; i < lengthBids; i++) {
        bids[i] = ['b', Number(bidSet[i]), Number(uniqueBuy[bidSet[i]])]
      }

      asks.sort((a, b) => a[1] - b[1])
      bids.sort((a, b) => b[1] - a[1])

      // Store best bids and asks per market
      const bestAskPrice: number = asks[0]?.[1] ? asks[0][1] : 0
      const bestBidPrice: number = bids[0]?.[1] ? bids[0][1] : 0

      const bestLiquidity = asks.concat(bids)
      redis.HSET(`bestask:${chainId}`, marketId, bestAskPrice)
      redis.HSET(`bestbid:${chainId}`, marketId, bestBidPrice)
      redis.SET(`bestliquidity:${chainId}:${marketId}`, JSON.stringify(bestLiquidity), { EX: 45 })

      let marketInfo: any = await redis.HGET(`marketinfo:${chainId}`, marketId)
      let cmcPrice = 0
      if (marketInfo) {
        marketInfo = JSON.parse(marketInfo)
        const baseAssetUsdPrice = Number(marketInfo.baseAsset.usdPrice)
        const quoteAssetUsdPrice = Number(marketInfo.quoteAsset.usdPrice)
        if (baseAssetUsdPrice > 0 && quoteAssetUsdPrice > 0) {
          cmcPrice = baseAssetUsdPrice / quoteAssetUsdPrice
        }
      }

      const bestAskPriceValid = bestAskPrice > 0 && (cmcPrice === 0 || Math.abs(cmcPrice - bestAskPrice) * 100 / cmcPrice <= 10) // diff < 10%
      const bestBidPriceValid = bestBidPrice > 0 && (cmcPrice === 0 || Math.abs(cmcPrice - bestBidPrice) * 100 / cmcPrice <= 10) // diff < 10%

      const midPrice: number =
        // eslint-disable-next-line no-nested-ternary
        bestAskPriceValid && bestBidPriceValid
          ? (bestAskPrice + bestBidPrice) / 2
          // eslint-disable-next-line no-nested-ternary
          : bestAskPriceValid
            ? bestAskPrice
            : bestBidPriceValid
              ? bestBidPrice : cmcPrice

      if (midPrice > 0) {
        redis.HSET(`lastprices:${chainId}`, marketId, formatPrice(midPrice));
      }


      // Clear old liquidity every 10 seconds
      redis.DEL(redisKeyLiquidity)
    })
    await Promise.all(results1)
  })
  await Promise.all(results0)
  console.timeEnd('removeOldLiquidity')
}

/**
 * Used to initialy fetch tokens infos on startup & updated on each recycle
 * @param chainId
 */
async function updateTokenInfoZkSync(chainId: number) {
  const updatedTokenInfo: AnyObject = {}

  // fetch new tokenInfo from zkSync
  let index = 0
  let tokenInfoResults: AnyObject[]
  const network = getNetwork(chainId)
  const cachedTokenInfos: AnyObject = await redis.HGETALL(`tokeninfo:${chainId}`)

  do {
    const fetchResult = await fetch(`${ZKSYNC_BASE_URL[network]}tokens?from=${index}&limit=100&direction=newer`).then((r: any) => r.json())
    tokenInfoResults = fetchResult.result.list
    const results1: Promise<any>[] = tokenInfoResults.map(async (tokenInfo: AnyObject) => {
      const { symbol, address } = tokenInfo
      if (!symbol || !address) return
      if (!symbol.includes('ERC20')) {
        try {
          const cachedName = address === ethers.constants.AddressZero
              ? 'Ethereum'
              : await redis.HGET(`tokenName:${chainId}`, symbol)
          if (cachedName) {
            tokenInfo.name = cachedName
          } else {
            const contract = new ethers.Contract(address, ERC20_ABI, ETHERS_PROVIDERS[chainId])
            tokenInfo.name = await contract.name()
            redis.HSET(`tokenName:${chainId}`, symbol, tokenInfo.name)
          }
        } catch (e: any) {
          console.warn(e.message)
          tokenInfo.name = tokenInfo.address
        }
        if (cachedTokenInfos[symbol]) {
          tokenInfo.usdPrice = JSON.parse(cachedTokenInfos[symbol]).usdPrice ?? 0
        } else {
          tokenInfo.usdPrice = 0
        }
        redis.HSET(`tokeninfo:${chainId}`, symbol, JSON.stringify(tokenInfo))
        updatedTokenInfo[symbol] = tokenInfo
      }
    })
    await Promise.all(results1)
    index = tokenInfoResults[tokenInfoResults.length - 1].id
  } while (tokenInfoResults.length > 99)

  // update existing marketInfo with the new tokenInfos
  const marketInfos = await redis.HGETALL(`marketinfo:${chainId}`)
  const resultsUpdateMarketInfos: Promise<any>[] = Object.keys(marketInfos).map(async (alias: string) => {
    const marketInfo = JSON.parse(marketInfos[alias])
    const [baseSymbol, quoteSymbol] = alias.split('-')
    if (!updatedTokenInfo[baseSymbol] || !updatedTokenInfo[quoteSymbol]) return

    marketInfo.baseAsset = updatedTokenInfo[baseSymbol]
    marketInfo.quoteAsset = updatedTokenInfo[quoteSymbol]
    await redis.HSET(`marketinfo:${chainId}`, alias, JSON.stringify(marketInfo))
  })
  await Promise.all(resultsUpdateMarketInfos)
}

/* update mm info after chainging the settings in EVMConfig */
async function updateEVMMarketInfo() {
  console.time('Update EVM marketinfo')

  const results0: Promise<any>[] = VALID_EVM_CHAINS.map(async (chainId: number) => {
    const evmConfig = EVMConfig[chainId]

    // check if settings changed
    const testPairString = await redis.HGET(`marketinfo:${chainId}`, 'WETH-USDC')
    let updated = false
    if (testPairString) {
      const marketInfo = JSON.parse(testPairString)
      if (marketInfo.exchangeAddress !== evmConfig.exchangeAddress) {
        console.log(`Updating marketinfo: ${marketInfo.exchangeAddress} -> ${evmConfig.exchangeAddress}`)
        updated = true
      }
      if (marketInfo.contractVersion !== evmConfig.domain.version) {
        console.log(`Updating contractVersion: ${marketInfo.contractVersion} -> ${evmConfig.domain.version}`)
        updated = true
      }
    }
    if (!updated) return

    // update all marketInfo
    const marketInfos = await redis.HGETALL(`marketinfo:${chainId}`)
    const markets = Object.keys(marketInfos)
    const results1: Promise<any>[] = markets.map(async (market: ZZMarket) => {
      if (!marketInfos[market]) return

      const marketInfo = JSON.parse(marketInfos[market])
      marketInfo.exchangeAddress = evmConfig.exchangeAddress
      marketInfo.contractVersion = evmConfig.domain.version
      marketInfo.baseFee = 0
      marketInfo.quoteFee = 0
      redis.HSET(`marketinfo:${chainId}`, market, JSON.stringify(marketInfo))
    })
    await Promise.all(results1)
  })
  await Promise.all(results0)
  console.timeEnd('Update EVM marketinfo')
}

async function cacheRecentTrades() {
  console.time('cacheRecentTrades')
  const results0: Promise<any>[] = VALID_CHAINS.map(async (chainId) => {
    const markets = await redis.SMEMBERS(`activemarkets:${chainId}`)
    const results1: Promise<any>[] = markets.map(async (marketId) => {
      const text =
        "SELECT chainid,id,market,side,price,amount,fill_status,txhash,taker_user_id,maker_user_id,feeamount,feetoken,insert_timestamp FROM fills WHERE chainid=$1 AND fill_status='f' AND market=$2 ORDER BY id DESC LIMIT 20"
      const query = {
        text,
        values: [chainId, marketId],
        rowMode: 'array',
      }
      const select = await db.query(query)
      redis.SET(`recenttrades:${chainId}:${marketId}`, JSON.stringify(select.rows))
    })
    await Promise.all(results1)
  })
  await Promise.all(results0)

  console.timeEnd('cacheRecentTrades')
}

async function deleteOldOrders() {
  console.time('deleteOldOrders')
  await db.query(
    "DELETE FROM offers WHERE order_status NOT IN ('o', 'pm', 'pf', 'b', 'm') AND update_timestamp < (NOW() - INTERVAL '100 DAYS')"
  )
  console.timeEnd('deleteOldOrders')
}

/* ################ V3 functions  ################ */

async function updatePriceHighLow() {
  console.time('updatePriceHighLow')

  const midnight = new Date(new Date().setUTCHours(0, 0, 0, 0)).toISOString()
  const selecUTC = await db.query(
    "SELECT chainid, market, MIN(price) AS min_price, MAX(price) AS max_price FROM fills WHERE insert_timestamp > $1 AND fill_status='f' AND chainid IS NOT NULL GROUP BY (chainid, market)",
    [midnight]
  )
  selecUTC.rows.forEach(async (row) => {
    const redisKeyLow = `price:utc:${row.chainid}:low`
    const redisKeyHigh = `price:utc:${row.chainid}:high`
    redis.HSET(redisKeyLow, row.market, row.min_price)
    redis.HSET(redisKeyHigh, row.market, row.max_price)
  })

  const oneDayAgo = new Date(Date.now() - 86400 * 1000).toISOString()
  const select = await db.query(
    "SELECT chainid, market, MIN(price) AS min_price, MAX(price) AS max_price FROM fills WHERE insert_timestamp > $1 AND fill_status='f' AND chainid IS NOT NULL GROUP BY (chainid, market)",
    [oneDayAgo]
  )
  select.rows.forEach(async (row) => {
    const redisKeyLow = `price:${row.chainid}:low`
    const redisKeyHigh = `price:${row.chainid}:high`
    redis.HSET(redisKeyLow, row.market, row.min_price)
    redis.HSET(redisKeyHigh, row.market, row.max_price)
  })

  // delete inactive markets
  VALID_CHAINS.forEach(async (chainId) => {
    const markets = await redis.SMEMBERS(`activemarkets:${chainId}`)
    const priceKeysLow = await redis.HKEYS(`price:${chainId}:low`)
    const delKeysLow = priceKeysLow.filter((k) => !markets.includes(k))
    delKeysLow.forEach(async (key) => {
      redis.HDEL(`price:${chainId}:low`, key)
    })
    const priceKeysHigh = await redis.HKEYS(`price:${chainId}:high`)
    const delKeysHigh = priceKeysHigh.filter((k) => !markets.includes(k))
    delKeysHigh.forEach(async (key) => {
      redis.HDEL(`price:${chainId}:high`, key)
    })
  })
  console.timeEnd('updatePriceHighLow')
}

async function updateVolumes() {
  console.time('updateVolumes')

  const midnight = new Date(new Date().setUTCHours(0, 0, 0, 0)).toISOString()
  const queryUTC = {
    text: `SELECT 
                chainid, market, 
                SUM(base_amount) AS base_volume, 
                SUM(quote_amount) AS quote_volume, 
                SUM(usd_notional) AS usd_volume 
                FROM past_orders 
                WHERE chainid IS NOT NULL AND txtime > $1  
                GROUP BY (chainid, market)`,
    values: [midnight],
  }
  const selectUTC = await db.query(queryUTC)
  selectUTC.rows.forEach(async (row) => {
    try {
      let quoteVolume = row.quote_volume.toPrecision(6)
      let baseVolume = row.base_volume.toPrecision(6)
      let usdVolume = row.usd_volume.toPrecision(6)
      // Prevent exponential notation
      if (quoteVolume.includes('e')) {
        quoteVolume = row.quote_volume.toFixed(0)
      }
      if (baseVolume.includes('e')) {
        baseVolume = row.base_volume.toFixed(0)
      }
      if (usdVolume.includes('e')) {
        usdVolume = row.usd_volume.toFixed(0)
      }
      const redisKeyBase = `volume:utc:${row.chainid}:base`
      const redisKeyQuote = `volume:utc:${row.chainid}:quote`
      const redisKeyUsd = `volume:utc:${row.chainid}:usd`
      redis.HSET(redisKeyBase, row.market, baseVolume)
      redis.HSET(redisKeyQuote, row.market, quoteVolume)
      redis.HSET(redisKeyUsd, row.market, usdVolume)
    } catch (err) {
      console.error(err)
      console.log('Could not update volumes')
    }
  })

  const oneDayAgo = new Date(Date.now() - 86400 * 1000).toISOString()
  const query = {
    text: `SELECT 
                chainid, market, 
                SUM(base_amount) AS base_volume, 
                SUM(quote_amount) AS quote_volume, 
                SUM(usd_notional) AS usd_volume 
                FROM past_orders 
                WHERE chainid IS NOT NULL AND txtime > $1  
                GROUP BY (chainid, market)`,
    values: [oneDayAgo],
  }
  const select = await db.query(query)
  select.rows.forEach(async (row) => {
    try {
      let quoteVolume = row.quote_volume.toPrecision(6)
      let baseVolume = row.base_volume.toPrecision(6)
      let usdVolume = row.usd_volume.toPrecision(6)
      // Prevent exponential notation
      if (quoteVolume.includes('e')) {
        quoteVolume = row.quote_volume.toFixed(0)
      }
      if (baseVolume.includes('e')) {
        baseVolume = row.base_volume.toFixed(0)
      }
      if (usdVolume.includes('e')) {
        usdVolume = row.usd_volume.toFixed(0)
      }
      const redisKeyBase = `volume:${row.chainid}:base`
      const redisKeyQuote = `volume:${row.chainid}:quote`
      const redisKeyUsd = `volume:${row.chainid}:usd`
      redis.HSET(redisKeyBase, row.market, baseVolume)
      redis.HSET(redisKeyQuote, row.market, quoteVolume)
      redis.HSET(redisKeyUsd, row.market, usdVolume)
    } catch (err) {
      console.error(err)
      console.log('Could not update volumes')
    }
  })

  try {
    // remove zero volumes
    VALID_CHAINS.forEach(async (chainId) => {
      const nonZeroMarkets = select.rows.filter((row) => row.chainid === chainId).map((row) => row.market)

      const baseVolumeMarkets = await redis.HKEYS(`volume:${chainId}:base`)
      const quoteVolumeMarkets = await redis.HKEYS(`volume:${chainId}:quote`)

      const keysToDelBase = baseVolumeMarkets.filter((m) => !nonZeroMarkets.includes(m))
      const keysToDelQuote = quoteVolumeMarkets.filter((m) => !nonZeroMarkets.includes(m))

      keysToDelBase.forEach((key) => {
        redis.HDEL(`volume:${chainId}:base`, key)
      })
      keysToDelQuote.forEach((key) => {
        redis.HDEL(`volume:${chainId}:quote`, key)
      })
    })
  } catch (err) {
    console.error(err)
    console.log('Could not remove zero volumes')
  }
  console.timeEnd('updateVolumes')
}

async function updateLastPrices() {
  console.time('updateLastPrices')

  const results0: Promise<any>[] = VALID_CHAINS.map(async (chainId) => {
    const redisKeyPriceInfo = `lastpriceinfo:${chainId}`

    const redisPrices = await redis.HGETALL(`lastprices:${chainId}`)
    const redisPricesQuote = await redis.HGETALL(`volume:${chainId}:quote`)
    const redisVolumesBase = await redis.HGETALL(`volume:${chainId}:base`)
    const markets = await redis.SMEMBERS(`activemarkets:${chainId}`)

    const results1: Promise<any>[] = markets.map(async (marketId) => {
      const marketInfo = await getMarketInfo(marketId, chainId).catch(() => null)
      if (!marketInfo) return
      const lastPriceInfo: any = {}
      const yesterday = new Date(Date.now() - 86400 * 1000).toISOString().slice(0, 10)
      const yesterdayPrice = Number(await redis.get(`dailyprice:${chainId}:${marketId}:${yesterday}`))
      lastPriceInfo.price = +redisPrices[marketId]
      lastPriceInfo.priceChange = yesterdayPrice ? Number(formatPrice(lastPriceInfo.price - yesterdayPrice)) : 0

      lastPriceInfo.quoteVolume = redisPricesQuote[marketId] || 0
      lastPriceInfo.baseVolume = redisVolumesBase[marketId] || 0

      redis.HSET(redisKeyPriceInfo, marketId, JSON.stringify(lastPriceInfo))
    })
    await Promise.all(results1)
  })
  await Promise.all(results0)
  console.timeEnd('updateLastPrices')
}

async function updateMarketSummarys() {
  console.time('updateMarketSummarys')

  const marketVolume = await db.query(`SELECT * FROM sum_market_volume`)
  const tradeCount24h = await db.query(`SELECT chainid, market, count(*) as count
                FROM past_orders 
                WHERE txtime > $1
                GROUP BY (chainid, market)`,
    [moment().subtract('1', 'd').toISOString()])

  const results0: Promise<any>[] = VALID_CHAINS.map(async (chainId) => {
    const redisKeyMarketSummary = `marketsummary:${chainId}`
    const redisKeyMarketSummaryUTC = `marketsummary:utc:${chainId}`

    // fetch needed data
    const redisVolumesQuote = await redis.HGETALL(`volume:${chainId}:quote`)
    const redisVolumesBase = await redis.HGETALL(`volume:${chainId}:base`)
    const redisVolumesUsd = await redis.HGETALL(`volume:${chainId}:usd`)
    const redisPrices = await redis.HGETALL(`lastprices:${chainId}`)
    const redisPricesLow = await redis.HGETALL(`price:${chainId}:low`)
    const redisPricesHigh = await redis.HGETALL(`price:${chainId}:high`)
    const redisBestAsk = await redis.HGETALL(`bestask:${chainId}`)
    const redisBestBid = await redis.HGETALL(`bestbid:${chainId}`)
    const markets = await redis.SMEMBERS(`activemarkets:${chainId}`)
    const redisVolumesQuoteUTC = await redis.HGETALL(`volume:utc:${chainId}:quote`)
    const redisVolumesBaseUTC = await redis.HGETALL(`volume:utc:${chainId}:base`)
    const redisVolumesUsdUTC = await redis.HGETALL(`volume:utc:${chainId}:usd`)
    const redisPricesLowUTC = await redis.HGETALL(`price:utc:${chainId}:low`)
    const redisPricesHighUTC = await redis.HGETALL(`price:utc:${chainId}:high`)

    const results1: Promise<any>[] = markets.map(async (marketId: ZZMarket) => {
      const marketInfo = await getMarketInfo(marketId, chainId).catch(() => null)
      if (!marketInfo) return
      const yesterday = new Date(Date.now() - 86400 * 1000).toISOString()
      const yesterdayPrice = Number(await redis.get(`dailyprice:${chainId}:${marketId}:${yesterday.slice(0, 10)}`))
      const today = new Date(Date.now()).toISOString()
      const todayPrice = Number(await redis.get(`dailyprice:${chainId}:${marketId}:${today.slice(0, 10)}`))

      const lastPrice = +redisPrices[marketId]

      let priceChange = 0
      let priceChangeUTC = 0
      let priceChangePercent_24h = 0
      let priceChangePercent_24hUTC = 0
      if (yesterdayPrice) {
        priceChange = Number(formatPrice(lastPrice - yesterdayPrice))
        priceChangePercent_24h = Number(formatPrice(priceChange / lastPrice))
      } else {
        priceChange = 0
        priceChangePercent_24h = 0
      }

      if (todayPrice) {
        priceChangeUTC = Number(formatPrice(lastPrice - todayPrice))
        priceChangePercent_24hUTC = Number(formatPrice(priceChangeUTC / lastPrice))
      } else {
        priceChangeUTC = 0
        priceChangePercent_24hUTC = 0
      }

      // get low/high price
      const lowestPrice_24h = Number(redisPricesLow[marketId])
      const highestPrice_24h = Number(redisPricesHigh[marketId])
      const lowestPrice_24hUTC = Number(redisPricesLowUTC[marketId])
      const highestPrice_24hUTC = Number(redisPricesHighUTC[marketId])

      // get volume
      const quoteVolume = Number(redisVolumesQuote[marketId] || 0)
      const baseVolume = Number(redisVolumesBase[marketId] || 0)
      const usdVolume = Number(redisVolumesUsd[marketId] || 0)
      const quoteVolumeUTC = Number(redisVolumesQuoteUTC[marketId] || 0)
      const baseVolumeUTC = Number(redisVolumesBaseUTC[marketId] || 0)
      const usdVolumeUTC = Number(redisVolumesUsdUTC[marketId] || 0)

      // get best ask/bid
      const lowestAsk = Number(formatPrice(redisBestAsk[marketId]))
      const highestBid = Number(formatPrice(redisBestBid[marketId]))

      const numberOfTrades_24h = tradeCount24h.rows.find(it => it.chainid === chainId && it.market === marketId)?.count ?? 0
      const numberOfTrades_24hUTC = numberOfTrades_24h

      const totalVolume = marketVolume.rows.find(it => it.chainid === chainId && it.market === marketId)?.usd_volume ?? 0

      const marketSummary: ZZMarketSummary = {
        market: marketId,
        baseSymbol: marketInfo.baseAsset.symbol,
        quoteSymbol: marketInfo.quoteAsset.symbol,
        lastPrice,
        lowestAsk,
        highestBid,
        baseVolume,
        quoteVolume,
        usdVolume24h: usdVolume,
        usdVolumeAll: totalVolume,
        priceChange,
        priceChangePercent_24h,
        highestPrice_24h,
        lowestPrice_24h,
        numberOfTrades_24h,
      }
      const marketSummaryUTC: ZZMarketSummary = {
        market: marketId,
        baseSymbol: marketInfo.baseAsset.symbol,
        quoteSymbol: marketInfo.quoteAsset.symbol,
        lastPrice,
        lowestAsk,
        highestBid,
        baseVolume: baseVolumeUTC,
        quoteVolume: quoteVolumeUTC,
        usdVolume24h: usdVolumeUTC,
        usdVolumeAll: totalVolume,
        priceChange: priceChangeUTC,
        priceChangePercent_24h: priceChangePercent_24hUTC,
        highestPrice_24h: highestPrice_24hUTC,
        lowestPrice_24h: lowestPrice_24hUTC,
        numberOfTrades_24h: numberOfTrades_24hUTC,
      }
      redis.HSET(redisKeyMarketSummaryUTC, marketId, JSON.stringify(marketSummaryUTC))
      redis.HSET(redisKeyMarketSummary, marketId, JSON.stringify(marketSummary))
    })
    await Promise.all(results1)
  })
  await Promise.all(results0)

  console.timeEnd('updateMarketSummarys')
}

let isRunningProcessAccVolume = false

async function updateAccVolume() {
  if (isRunningProcessAccVolume) return;
  isRunningProcessAccVolume = true;
  console.time("updateAccVolume");
  let pastOrders;
  try {
    const chainId = 1;
    const keyLastProceedPastOrderId = "last_proceed_past_order_id3";
    const workerPrefs = `acc_vol_worker:${chainId}`;
    const lastProceedPastOrderId = Number(await redis.HGET(workerPrefs, keyLastProceedPastOrderId) ?? 0);

    pastOrders = await db.query(`
      SELECT * FROM past_orders
      WHERE chainid = $1 AND id > $2
      ORDER BY id ASC LIMIT 100
    `, [chainId, lastProceedPastOrderId]);

    if (pastOrders.rowCount <= 0) return;

    for (let i = 0; i < pastOrders.rows.length; i++) {
      const row = pastOrders.rows[i];
      const createdAt = moment();
      const tableName = "account_volume";
      const update = await db.query(`
        INSERT INTO ${tableName} 
        (chainid, address, total_usd_vol, last_past_order_id,
        total_trade_count, created_at)
        VALUES ($1, $2, $3, $4, 1, $5)
        ON CONFLICT (chainid, address)
        DO UPDATE SET
          total_usd_vol = ${tableName}.total_usd_vol + $3,
          total_trade_count = ${tableName}.total_trade_count + 1,
          last_past_order_id = $4
        WHERE ${tableName}.last_past_order_id < $4
        RETURNING id, ref_code, ref_status, total_usd_vol, total_trade_count, created_at
      `, [
        chainId, row.taker_address, row.usd_notional, row.id, createdAt.toISOString()
      ]);
      if (update.rows.length < 1) {
        console.warn(`Detect duplicate update acc_volume pass_order.id = ${row.id}`);
      }
    }
    await redis.HSET(
      workerPrefs, keyLastProceedPastOrderId,
      `${pastOrders.rows[pastOrders.rows.length - 1].id}`);
  } finally {
    isRunningProcessAccVolume = false;
    if (pastOrders?.rowCount) {
      console.log(`updateAccVolume proceed ${pastOrders?.rowCount} orders`);
    }
    console.timeEnd("updateAccVolume");
  }
}

async function runDbMigration() {
  console.log('running db migration')
  const migration = fs.readFileSync('schema.sql', 'utf8')
  await db.query(migration).catch(console.error)
}

async function start() {
  console.log('background.ts: Run checks')

  console.log('background.ts: Run startup')

  await redis.connect()
  await publisher.connect()
  await runDbMigration()

  // fetch abi's
  ERC20_ABI = JSON.parse(fs.readFileSync('abi/ERC20.abi', 'utf8'))
  EVMConfig = JSON.parse(fs.readFileSync('EVMConfig.json', 'utf8'))
  const EVMContractABI = JSON.parse(fs.readFileSync('abi/EVM_Exchange.json', 'utf8')).abi

  // connect infura providers
  const operatorKeysString = process.env.ALCHEMY_API_KEY as any
  if (!operatorKeysString && VALID_EVM_CHAINS.length) throw new Error("MISSING ENV VAR 'ALCHEMY_API_KEY'")

  const results: Promise<any>[] = VALID_CHAINS.map(async (chainId: number) => {
    if (ETHERS_PROVIDERS[chainId]) return
    ETHERS_PROVIDERS[chainId] = new ethers.providers.AlchemyProvider(getNetwork(chainId), operatorKeysString)

    if (chainId === 1) {
      SYNC_PROVIDER.mainnet = await zksync.getDefaultRestProvider('mainnet')
    }
    if (chainId === 1002) {
      SYNC_PROVIDER.goerli = await zksync.getDefaultRestProvider('goerli')
    }
  })
  Promise.all(results)

  ZKSYNC_BASE_URL.mainnet = 'https://api.zksync.io/api/v0.2/'
  ZKSYNC_BASE_URL.goerli = 'https://goerli-api.zksync.io/api/v0.2/'

  /* startup */
  await updateEVMMarketInfo()
  try {
    const updateResult = VALID_CHAINS_ZKSYNC.map(async (chainId) => updateTokenInfoZkSync(chainId))
    await Promise.all(updateResult)
  } catch (e: any) {
    console.error(`Failed to updateTokenInfoZkSync: ${e}`)
  }

  console.log('background.ts: Starting Update Functions')
  setInterval(updatePendingOrders, updatePendingOrdersDelay * 1000)
  setInterval(cacheRecentTrades, 60000)
  setInterval(removeOldLiquidity, 10000)
  setInterval(updateLastPrices, 15000)
  setInterval(updateMarketSummarys, 15000)
  updateUsdPrice().then()
  setInterval(updateUsdPrice, 15 * 60 * 1000)
  setInterval(updateFeesZkSync, 25000)
  setInterval(updatePriceHighLow, 30000)
  setInterval(updateVolumes, 30000)
  setInterval(deleteOldOrders, 60 * 60 * 1000)
  setInterval(updateAccVolume, 5000)

  launchTgBot()
}

process.once("SIGINT", () => setTimeout(() => process.exit(0), 3000));
process.once("SIGTERM", () => process.exit(1));

start()
