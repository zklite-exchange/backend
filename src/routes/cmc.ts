import type { AnyObject, ZZHttpServer } from "src/types";
import { redis } from "src/redisClient";
import { CMC_IDS } from "src/utils";

export default function cmcRoutes(app: ZZHttpServer) {
  const defaultChainId = process.env.DEFAULT_CHAIN_ID ? Number(process.env.DEFAULT_CHAIN_ID) : 1

  function getChainId(req: any, res: any, next: any) {
    const chainId = req.params.chainId ? Number(req.params.chainId) : defaultChainId

    req.chainId = chainId
    next()
  }

  app.get('/api/coinmarketcap/v1/markets/:chainId?', getChainId, async (req, res) => {
    try {
      const { chainId } = req

      if (!chainId || !app.api.VALID_CHAINS.includes(chainId)) {
        res.status(400).send({
          op: 'error',
          message: `ChainId not found, use ${app.api.VALID_CHAINS}`,
        })
        return
      }

      const markets: any = {}
      const marketSummarys: any = await app.api.getMarketSummarys(chainId)

      Object.keys(marketSummarys).forEach((market: string) => {
        const marketSummary = marketSummarys[market]
        const entry: any = {
          trading_pairs: marketSummary.market,
          base_currency: marketSummary.baseSymbol,
          quote_currency: marketSummary.quoteSymbol,
          last_price: marketSummary.lastPrice,
          lowest_ask: marketSummary.lowestAsk,
          highest_bid: marketSummary.highestBid,
          base_volume: marketSummary.baseVolume,
          quote_volume: marketSummary.quoteVolume,
          price_change_percent_24h: marketSummary.priceChangePercent_24h,
          highest_price_24h: marketSummary.highestPrice_24h,
          lowest_price_24h: marketSummary.lowestPrice_24h,
        }
        markets[market] = entry
      })
      res.status(200).json(markets)
    } catch (error: any) {
      console.log(error.message)
      res.status(400).send({ op: 'error', message: 'Failed to fetch markets' })
    }
  })

  app.get('/api/coinmarketcap/v1/ticker/:chainId?', getChainId, async (req, res) => {
    try {
      const { chainId } = req

      if (!chainId || !app.api.VALID_CHAINS.includes(chainId)) {
        res.status(400).send({
          op: 'error',
          message: `ChainId not found, use ${app.api.VALID_CHAINS}`,
        })
        return
      }

      const ticker: any = {}
      const lastPrices: any = await app.api.getLastPrices(chainId)
      lastPrices.forEach((price: string[]) => {
        const entry: any = {
          last_price: price[1],
          base_volume: price[4],
          quote_volume: price[3],
          isFrozen: 0,
        }
        ticker[price[0]] = entry
      })
      res.status(200).json(ticker)
    } catch (error: any) {
      console.log(error.message)
      res.status(400).send({ op: 'error', message: 'Failed to fetch ticker prices' })
    }
  })
  app.get('/api/coinmarketcap/v1/assets/:chainId?', getChainId, async (req, res) => {
    try {
      const { chainId } = req

      if (!chainId || !app.api.VALID_CHAINS.includes(chainId)) {
        res.status(400).send({
          op: 'error',
          message: `ChainId not found, use ${app.api.VALID_CHAINS}`,
        })
        return
      }

      const markets = await redis.SMEMBERS(`activemarkets:${chainId}`)
      const tokenInfos: AnyObject = await redis.HGETALL(`tokeninfo:${chainId}`)
      // get active tokens once
      const tokenSymbols: string[] = Array.from(new Set(markets.join('-').split('-')))
      const assets: AnyObject = { }
      for (let i = 0; i < tokenSymbols.length; i++) {
        const symbol = tokenSymbols[i]
        const tokenInfo = JSON.parse(tokenInfos[symbol] || '{}')
        const name = tokenInfo?.name
        const address = tokenInfo?.address
        const cmcId = CMC_IDS[symbol]
        if (name && address && cmcId) {
          assets[symbol] = {
            "name": name,
            "unified_cryptoasset_id": cmcId,
            "can_withdraw": true,
            "can_deposit": true,
            "contractAddress": address,
          }
        }
      }
      res.status(200).json(assets);
    } catch (error: any) {
      console.log(error.message)
      res.status(400).send({ op: 'error', message: 'Failed to fetch assets' })
    }
  })

  app.get('/api/coinmarketcap/v1/orderbook/:chainId/:marketPair?', async (req, res) => {
    let { chainId } = req.params as any
    const { marketPair } = req.params as any
    chainId = Number(chainId)

    if (!chainId || !app.api.VALID_CHAINS.includes(chainId)) {
      res.status(400).send({
        op: 'error',
        message: `ChainId not found, use ${app.api.VALID_CHAINS}`,
      })
      return
    }

    if (!marketPair) {
      res.status(400).send({
        op: 'error',
        message: 'Invalid market pair'
      })
      return
    }

    const market = marketPair.replace('_', '-').toUpperCase()
    const altMarket = marketPair.replace('_', '-')
    const depth: number = req.query.depth ? Number(req.query.depth) : 0
    const level: number = req.query.level ? Number(req.query.level) : 2
    if (![1, 2, 3].includes(level)) {
      res.status(400).send({
        op: 'error',
        message: `Level: ${level} is not a valid level. Use 1, 2 or 3.`,
      })
      return
    }

    try {
      // get data
      let orderBook = await app.api.getOrderBook(chainId, market, depth, level)
      if (orderBook.asks.length === 0 && orderBook.bids.length === 0) {
        orderBook = await app.api.getOrderBook(chainId, altMarket, depth, level)
      }
      res.status(200).json(orderBook)
    } catch (error: any) {
      console.log(error.message)
      res.status(400).send({
        op: 'error',
        message: `Failed to fetch orderbook for ${market}, ${error.message}`,
      })
    }
  })

  app.get('/api/coinmarketcap/v1/trades/:chainId/:marketPair?', async (req, res) => {
    let { chainId } = req.params as any
    const { marketPair } = req.params as any
    chainId = Number(chainId)

    if (!chainId || !app.api.VALID_CHAINS.includes(chainId)) {
      res.status(400).send({
        op: 'error',
        message: `ChainId not found, use ${app.api.VALID_CHAINS}`,
      })
      return
    }

    if (!marketPair) {
      res.status(400).send({
        op: 'error',
        message: 'Invalid market pair'
      })
      return
    }

    const market = marketPair.replace('_', '-').toUpperCase()
    const altMarket = marketPair.replace('_', '-')
    try {
      let fills = await app.api.getfills(chainId, market, 25)
      if (fills.length === 0) {
        fills = await app.api.getfills(chainId, altMarket, 25)
      }

      if (fills.length === 0) {
        res.status(400).send({ op: 'error', message: `Can not find trades for ${market}` })
        return
      }

      const response: any[] = []
      fills.forEach((fill) => {
        const date = new Date(fill[12])
        const entry: any = {
          trade_id: fill[1],
          price: fill[4],
          base_volume: fill[5],
          quote_volume: fill[5] * fill[4],
          timestamp: date.getTime(),
          type: fill[3] === 's' ? 'sell' : 'buy',
        }
        response.push(entry)
      })

      res.status(200).send(response)
    } catch (error: any) {
      console.log(error.message)
      res.status(400).send({
        op: 'error',
        message: `Failed to fetch trades for ${market}`,
      })
    }
  })
}
