import type { ZZServiceHandler } from 'src/types'

export const cancelall: ZZServiceHandler = async (
  api,
  ws,
  [chainid, userid]
) => {
  const userconnkey = `${chainid}:${userid}`
  if (api.USER_CONNECTIONS[userconnkey] !== ws) {
    ws.send(
      JSON.stringify({
        op: 'error',
        args: ['cancelall', userid, 'Unauthorized'],
      })
    )
  }
  const canceled_orders = await api.cancelallorders(userid)
  const orderupdates = canceled_orders.map((orderid: string) => [
    chainid,
    orderid,
    'c',
  ])
  await api.broadcastMessage(chainid, null, {
    op: 'orderstatus',
    args: [orderupdates],
  })
}