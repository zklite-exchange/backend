// SPDX-License-Identifier: BUSL-1.1
import type { Application } from 'express'
import type { WebSocket, WebSocketServer } from 'ws'
import type API from 'src/api'

export type AnyObject = { [key: string | number]: any }

export type ZZMarket = string

export type ZZMarketInfo = {
  [key: string]: any
}

export type ZZFillOrder = {
  amount: number
  accountId: string
}

export type ZZMarketSide = 'b' | 's'

export type ZkTx = {
  accountId: string
  tokenSell: string
  tokenBuy: string
  nonce: string
  ratio: [number, number]
  amount: number
  validUntil: number
  recipient: string
}

export type ZZMarketSummary = {
  market: string
  baseSymbol: string
  quoteSymbol: string
  lastPrice: number
  lowestAsk: number
  highestBid: number
  baseVolume: number
  quoteVolume: number
  usdVolume24h: number
  usdVolumeAll: number
  priceChange: number
  priceChangePercent_24h: number
  highestPrice_24h: number
  lowestPrice_24h: number
  numberOfTrades_24h: number
}

export type ZZOrder = {
  user: string
  sellToken: string
  buyToken: string
  sellAmount: string
  buyAmount: string
  expirationTimeSeconds: string
  signature?: string
}

/* ################ V3 functions  ################ */
export type WSMessage = {
  op: string
  args: any[]
}

export type WSocket = WebSocket & {
  uuid: string
  isAlive: boolean
  marketSubscriptions: string[]
  chainId: number
  userId: string
  origin: string
  swapEventSubscription: string | null
}

export type ZZAPITransport = { api: API }
export type ZZServiceHandler = (api: API, ws: WSocket, args: any[]) => any
export type ZZSocketServer = WebSocketServer & ZZAPITransport
export type ZZHttpServer = Application & ZZAPITransport

export type ZZPastOrder = {
  chainId: number
  taker: string
  maker: string
  makerSellToken: string
  takerSellToken: string
  takerBuyAmount: number
  takerSellAmount: number
  makerFee: number
  takerFee: number
  transactionHash: string
  transactionTime: number
}

// eslint-disable-next-line no-shadow
export enum RefereeStatus {
  OLD = 'old',
  NEW = 'new',
  IN_REVIEW = 'in_review',
  REJECTED = 'rejected',
  APPROVED = 'approved',
}
export const REF_CODE_ORGANIC = 'organic'
export const REF_ADDRESS_ORGANIC = '0xnull'
export const REF_MIN_USD = 200
export const REF_MIN_TRADE_COUNT = 3
export const REF_VOL_COMMISSION_RATE = 0.1 / 100
export const REF_VOL_COMMISSION_MAX = 100