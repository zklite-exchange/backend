import API from 'src/api'
import type { ZZHttpServer } from 'src/types'

export default function cmcRoutes(app: ZZHttpServer) {
  app.get('/all', async (req, res) => {
    try {
      const marketSummarys: any =  await app.api.getMarketSummarys(1000)
      res.json(marketSummarys)
    } catch (error: any) {
      console.log(error.message)
      res.send({ op: 'error', message: 'Failed to fetch markets' })
    }
  })

  app.get('/ticker', async (req, res) => {
    try {
      const ticker: any = {}
      const lastPrices: any =  await app.api.getLastPrices(1000)
      lastPrices.forEach((price: string[]) => {
        ticker[price[0]] = {
          "last_price": price[1],
          "quote_volume": price[3],
          "base_volume": price[4],
          "isFrozen": 0
        }
      })
      res.json(lastPrices)
    } catch (error: any) {
      console.log(error.message)
      res.send({ op: 'error', message: 'Failed to fetch ticker prices' })
    }
  })

  app.get('/orderbook/:market_pair', async (req, res) => {
    const market = (req.params.market_pair).replace('_','-') 
    try {           
      const timestamp = Date.now()
      const liquidity = await app.api.getLiquidity(
        1000,
        market
      )
      const bids = liquidity
        .filter((l) => l[0] === 'b')
        .map((l) => [l[1],l[2]])
        .reverse()

      const asks = liquidity
        .filter((l) => l[0] === 's')
        .map((l) => [l[1],l[2]])

      res.json({
        "timestamp": timestamp,
        "bids": bids,
        "asks": asks
      })
    } catch (error: any) {
      console.log(error.message)
      res.send({ op: 'error', message: `Failed to fetch orderbook for ${market}` })
    }
  })

  app.get('/trades/:market_pair', async (req, res) => {
    const market = (req.params.market_pair).replace('_','-') 
    try {
      const fills = await app.api.getfills(
        1000,
        market
      )

      const response: any[] = []
      fills.forEach(fill => {
        const date = new Date(fill.insert_timestamp)
        response.push(
          {
            "trade_id": fill.id,
            "price": fill.price,
            "base_volume": fill.amount,
            "quote_volume": (fill.amount * fill.price),
            "timestamp": date.getTime(),
            "type": (fill.side === 's') ? 'sell' : 'buy',
          }
        )
      })

      res.send(response)
    } catch (error: any) {
      console.log(error.message)
      res.send({ op: 'error', message: `Failed to fetch trades for ${market}` })
    }
  })
}
