import * as starknet from 'starknet'
import * as Sentry from '@sentry/node'
import { ethers } from "ethers";
import { randomBytes } from 'crypto'

export function formatPrice(input: any) {
  const inputNumber = Number(input)
  if (inputNumber > 99999) {
    return Number(inputNumber.toFixed(0)).toString()
  }
  if (inputNumber > 9999) {
    return Number(inputNumber.toFixed(1)).toString()
  }
  if (inputNumber > 999) {
    return Number(inputNumber.toFixed(2)).toString()
  }
  if (inputNumber > 99) {
    return Number(inputNumber.toFixed(3)).toString()
  }
  if (inputNumber > 9) {
    return Number(inputNumber.toFixed(4)).toString()
  }
  if (inputNumber > 0.1) {
    return Number(inputNumber.toFixed(5)).toString()
  }
  if (inputNumber > 0.01) {
    return Number(inputNumber.toFixed(6)).toString()
  }
  if (inputNumber > 0.001) {
    return Number(inputNumber.toFixed(7)).toString()
  }
  if (inputNumber > 0.00001) {
    return Number(inputNumber.toFixed(8)).toString()
  }
  return Number(inputNumber.toFixed(10)).toString()
}

export function stringToFelt(text: string) {
  const bufferText = Buffer.from(text, 'utf8')
  const hexString = `0x${bufferText.toString('hex')}`
  return starknet.number.toFelt(hexString)
}

export function getNetwork(chainId: number) {
  switch (chainId) {
    case 1:
      return 'mainnet'
    case 1002:
    case 1001:
      return 'goerli'
    case 42161:
      return 'arbitrum'
    default:
      throw new Error('No valid chainId')
  }
}

export function getRPCURL(chainId: number) {
  switch (chainId) {
    case 42161:
      return 'https://arb1.arbitrum.io/rpc'
    case 421613:
      return 'https://goerli-rollup.arbitrum.io/rpc'
    default:
      throw new Error('No valid chainId')
  }
}

/**
 * Get the full token name from L1 ERC20 contract
 * @param provider
 * @param contractAddress
 * @param abi
 * @returns tokenInfos
 */
export async function getERC20Info(provider: any, contractAddress: string, abi: any) {
  const contract = new ethers.Contract(contractAddress, abi, provider)
  const [decimalsRes, nameRes, symbolRes] = await Promise.allSettled([contract.decimals(), contract.name(), contract.symbol()])

  const tokenInfos: any = { address: contractAddress }
  tokenInfos.decimals = decimalsRes.status === 'fulfilled' ? decimalsRes.value : null
  tokenInfos.name = nameRes.status === 'fulfilled' ? nameRes.value : null
  tokenInfos.symbol = symbolRes.status === 'fulfilled' ? symbolRes.value : null

  return tokenInfos
}

export function getNewToken() {
  return randomBytes(64).toString('hex')
}

export function getFeeEstimationMarket(chainId: number) {
  switch (chainId) {
    case 42161:
      return 'USDC-USDT'
    case 421613:
      return 'DAI-USDC'
    default:
      throw new Error('No valid chainId')
  }
}

export function getReadableTxError(errorMsg: string): string {
  if (errorMsg.includes('orders not crossed')) return 'orders not crossed'

  if (errorMsg.includes('mismatched tokens')) return 'mismatched tokens'

  if (errorMsg.includes('invalid taker signature')) return 'invalid taker signature'

  if (errorMsg.includes('invalid maker signature')) return 'invalid maker signature'

  if (errorMsg.includes('taker order not enough balance')) return 'taker order not enough balance'

  if (errorMsg.includes('maker order not enough balance')) return 'maker order not enough balance'

  if (errorMsg.includes('taker order not enough balance for fee')) return 'taker order not enough balance for fee'

  if (errorMsg.includes('maker order not enough balance for fee')) return 'maker order not enough balance for fee'

  if (errorMsg.includes('order is filled')) return 'order is filled'

  if (errorMsg.includes('order expired')) return 'order expired'

  if (errorMsg.includes('order canceled')) return 'order canceled'

  if (errorMsg.includes('self swap not allowed')) return 'self swap not allowed'

  if (errorMsg.includes('ERC20: transfer amount exceeds allowance')) return 'ERC20: transfer amount exceeds allowance'

  // this might be a new error, log it
  console.log(`getReadableTxError: unparsed error: ${errorMsg}`)
  return 'Internal error: A'
}

export function sortMarketPair(tokenInputA: string, tokenInputB: string): string {
  const tokenA = ethers.BigNumber.from(tokenInputA)
  const tokenB = ethers.BigNumber.from(tokenInputB)
  if (tokenA.lt(tokenB)) {
    return `${tokenInputA.toLowerCase()}-${tokenInputB.toLowerCase()}`
  }

  return `${tokenInputB.toLowerCase()}-${tokenInputA.toLowerCase()}`
}

export const RE_REF_CODE = /^[a-zA-Z0-9_]{5,15}$/
export const RE_ETHER_ADDRESS = /^0x[a-fA-F0-9]{40}$/

export const CMC_IDS: Record<string, number> = {
  "BTC": 1,
  "ETH":  1027,
  "USDC": 3408,
  "USDT": 825,
  "DAI": 4943,
  "LINK": 1975,
  "STORJ": 1772,
  "UNI": 7083,
  "WBTC": 3717,
  "WETH": 2396,
  "wstEth": 12409,
  "FXS": 6953,
  "SOL": 5426,
  "DYDX": 28324,
  "MATIC": 3890,
  "FTM": 3513,
  "LDO": 8000,
  "FRAX": 6952,
  "AAVE": 7278,
  "ENS": 13855,
  "SHIB": 5994,
  "ZZ": 20755,
}

export function captureError(error: any, details: {tags?: Record<string, any>, req?: any, [key: string]: any} = {}): boolean {
  Sentry.withScope(scope => {
    if (details?.tags) {
      scope.setTags(details.tags)
      delete details.tags;
    }
    if (details?.req) {
      const {req} = details;
      delete details.req;
      scope.addEventProcessor(event => Sentry.addRequestDataToEvent(event, req))
    }

    if (details) scope.setExtras(details)
    scope.captureException(error)
  })
  return false
}