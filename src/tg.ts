import { Telegraf } from "telegraf";
import * as console from "console";
import { redis } from "src/redisClient";
import db from "src/db";
import { REF_CODE_ORGANIC } from "src/types";
import { captureError, RE_ETHER_ADDRESS, RE_REF_CODE } from "src/utils";
import type { ExtraReplyMessage } from "telegraf/typings/telegram-types";
import { bold, code, fmt, type FmtString, italic, link } from "telegraf/format";
import fetch from "isomorphic-fetch";

const bot = new Telegraf(process.env.TG_BOT_TOKEN as string);

const commandDesc = [{
  command: "/help",
  description: "Print help message"
}, {
  command: "/notification",
  description: "Register order status notification"
}];


type TgConversationContext = {
  type: string;
  [key: string]: any
}

export function concatFmt(...fmts: (string | FmtString)[]): FmtString {
  if (fmts.length <= 0) return fmt``;
  let res = fmt`${fmts[0]}`;
  for (let i = 1; i < fmts.length; i++) {
    res = fmt`${res}${fmts[i]}`;
  }
  return res;
}

function updateConversationContext(chatId: number, data: TgConversationContext | null) {
  if (data == null) return redis.DEL(`tg_context:${chatId}`);
  return redis.SET(`tg_context:${chatId}`, JSON.stringify(data), { EX: 15 * 60 });
}

async function getConversationContext(chatId: number): Promise<TgConversationContext | undefined> {
  const json = await redis.GET(`tg_context:${chatId}`);
  return json ? JSON.parse(json) as TgConversationContext : undefined;
}

export async function launchTgBot() {
  bot.start((ctx) => ctx.reply("👋 Welcome, I'm zkLite Exchange Chatbot! /help to see available commands. \nzklite.io"));
  bot.help(async (ctx) => {
    await updateConversationContext(ctx.chat.id, null);
    await ctx.replyWithMarkdownV2(`*zkLite Exchange Bot*\n${
      commandDesc.map(it => `${it.command.replace("_", "\\_")} \\- ${it.description}`).join("\n")
    }`);
  });

  bot.command("notification", async (ctx) => {
    await updateConversationContext(ctx.chat.id, null);
    const match = /\/notification\s+enable\s+([^\s]+)/.exec(ctx.text)
    if (!match) {
      ctx.reply("👉 To enable notification, please copy Telegram command from:\n" +
        "https://trade.zklite.io/notification", {link_preview_options: {is_disabled: true}})
    } else {
      const deviceAlias = match[1];
      const isValid = (await db.query(`SELECT 1 FROM devices WHERE alias = $1`, [deviceAlias])).rowCount > 0
      if (!isValid) {
        ctx.reply("❌ Invalid code, please copy command from:\nhttps://trade.zklite.io/notification", {
          link_preview_options: {is_disabled: true}
        })
      } else {
        await db.query(
          `INSERT INTO device2noti (device_alias, tg_id) VALUES ($1, $2) ON CONFLICT DO NOTHING`,
          [deviceAlias, `${ctx.chat.id}`]
        )
        ctx.reply('✅ Enabled notification, now you will receive order update via this bot.')
      }
    }
  });


  await bot.telegram.setChatMenuButton({
    menuButton: {
      type: "commands"
    }
  });
  await bot.telegram.setMyCommands(commandDesc);
  bot.launch().then();

  process.once("SIGINT", () => bot.stop("SIGINT"));
  process.once("SIGTERM", () => bot.stop("SIGTERM"));
}

type UserAndMsgOpts = {
  chainId?: number, address?: string,
  device_alias?: string
} & ExtraReplyMessage;

export async function notifyUser(msg: string | FmtString, opts: UserAndMsgOpts) {
  if (!opts || !opts.chainId && !opts.address && !opts.device_alias) {
    return;
  }
  const queryRes = await db.query(`
    SELECT DISTINCT tg_id FROM device2noti
    LEFT JOIN address2device
    ON address2device.device_alias = device2noti.device_alias
    WHERE (address2device.chainid = $1 AND address2device.address = $2) OR (device2noti.device_alias = $3)
  `, [opts.chainId, opts.address, opts.device_alias]);
  for (let i = 0; i < queryRes.rows.length; i++) {
    const chatId = queryRes.rows[i].tg_id;
    if (!chatId) return;
    await bot.telegram.sendMessage(Number(chatId), msg, opts);
  }
}

export async function notifyReferrerNewRef(chainId: number, referrerAddress: string, refCode: string, address: string) {
  const msg = fmt`🎉 A new ${
    link("address", `https://zkscan.io/explorer/accounts/${address}`)
  } has been connected zklite.io using your referral link (REF_CODE: ${code(refCode)})`;
  notifyUser(msg, {
    chainId, address: referrerAddress
  });
}

