import { Telegraf } from "telegraf";
import * as console from "console";
import { redis } from "src/redisClient";
import db from "src/db";
import { REF_CODE_ORGANIC } from "src/types";
import { captureError, RE_REF_CODE } from "src/utils";
import type { ExtraReplyMessage } from "telegraf/typings/telegram-types";
import { bold, code, fmt, type FmtString, italic, link } from "telegraf/format";
import fetch from "isomorphic-fetch";

const bot = new Telegraf(process.env.TG_BOT_TOKEN as string);

const commandDesc = [{
  command: "/help",
  description: "Print help message"
}, {
  command: "/noti",
  description: "Register order status notification"
}, {
  command: "/ref_add",
  description: "Register a new Referral link"
}, {
  command: "/ref_links",
  description: "View your Referral links"
}];

const CONTEXT_TYPE_REF_ADD = "ref_add";

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
  bot.command("ref_add", async (ctx) => {
    try {
      await ctx.reply(concatFmt(
        fmt`Please enter your ${code`REF_CODE`} (only alphabet characters, numbers, and _ are valid)\n\n`,
        "Your Referral link will be:\n",
        code`https://zklite.io/?referrer=REF_CODE`
      ));
      await updateConversationContext(ctx.chat.id, { type: CONTEXT_TYPE_REF_ADD });
    } catch (e) {
      captureError(e);
      console.error(e, "/ref_add error");
    }
  });
  bot.command("ref_links", async (ctx) => {
    const chatId = ctx.chat.id;
    const query = await db.query(`SELECT * FROM referrers WHERE tg_chat_id = $1 ORDER BY click_count DESC`, [`${chatId}`]);
    if (query.rowCount === 0) {
      ctx.reply("You haven't created any referral links.\nYou can create one by /ref_add");
      return;
    }
    let msg = fmt`${bold`(Click count) - Referral links`}`;
    for (let i = 0; i < query.rows.length; i++) {
      const row = query.rows[i];
      msg = fmt`${msg}\n(${row.click_count}) - ${code`https://zklite.io/?referrer=${row.code}`}`;
    }
    ctx.reply(msg);
  });
  bot.command("noti", async (ctx) => {
    await updateConversationContext(ctx.chat.id, null);
    ctx.reply("Sorry this command is still in development!");
  });
  bot.on("message", async (ctx) => {
    const chatId = ctx.chat.id;
    const conversationContext = await getConversationContext(chatId);
    if (conversationContext?.type === CONTEXT_TYPE_REF_ADD) {
      let { refCode } = conversationContext;
      if (refCode == null) {
        refCode = ctx.text;
        if (!refCode) return;
        if (!/^[a-zA-Z0-9_]+$/.test(refCode)) {
          ctx.replyWithMarkdownV2("Your `REF_CODE` contains invalid characters, please type again");
          return;
        }
        if (refCode.length < 5) {
          ctx.replyWithMarkdownV2("Your `REF_CODE` is too short, please type again");
          return;
        }
        if (refCode.length > 15) {
          ctx.replyWithMarkdownV2("Your `REF_CODE` is too long, please type again");
          return;
        }
        const isTaken = refCode === REF_CODE_ORGANIC ||
          (await db.query("SELECT 1 FROM referrers WHERE code = $1", [refCode]))
            .rowCount > 0;
        if (isTaken) {
          ctx.replyWithMarkdownV2("Your `REF_CODE` is taken, please choose another one");
          return;
        }
        await updateConversationContext(chatId, {
          type: CONTEXT_TYPE_REF_ADD,
          refCode
        });
        ctx.reply(fmt`Please enter your ${bold`zkSync Lite`} wallet address, your reward will be sent to this address.\n${
          italic`(you should use a single wallet address for all your referral links)`
        }`);
      } else {
        const address = ctx.text;
        if (!address || !/^0x[a-fA-F0-9]{40}/.test(address)) {
          ctx.reply("This is not a valid zkSync Lite address, please type again");
          return;
        }

        await updateConversationContext(chatId, null);

        try {
          const accountPubKeyHash = await fetch(`https://api.zksync.io/api/v0.2/accounts/${address}`)
            .then((r: any) => r.json())
            .then((data: any) => data.result?.committed?.pubKeyHash);
          if (!accountPubKeyHash) {
            ctx.reply(fmt`This ${
              link("address", `https://zkscan.io/explorer/accounts/${address}`)
            } doesn't exist on zkSync Lite (missing pubKey), please set pubKey or use another address.`, {
              link_preview_options: { is_disabled: true }
            });
            return;
          }
        } catch (e) {
          captureError(e, { address });
          ctx.reply("Something went wrong!");
          return;
        }

        if (!RE_REF_CODE.test(refCode)) {
          ctx.reply("Something went wrong!");
          return;
        }
        try {
          await db.query(`
            INSERT INTO referrers (chainid, address, code, tg_chat_id) VALUES ($1, $2, $3, $4)
          `, [1, address, refCode, `${chatId}`]);
          ctx.reply(concatFmt(
            "Create Referral link successfully, your link is:\n",
            `https://zklite.io?referrer=${refCode}\n\n`,
            fmt`Your reward will be sent to: ${link(address, `https://zkscan.io/explorer/accounts/${address}`)}\n\n`,
            "Please read the Referral program documents for the prize, terms and conditions:\n",
            "https://docs.zklite.io/referral-program#heres-how-it-works"
          ), {
            link_preview_options: {
              is_disabled: true
            }
          });
        } catch (e: any) {
          if (e.message?.includes("referrers_chainid_code")) {
            ctx.reply(`Error, your REF_CODE (${refCode}) is already taken, please try /ref_add again!`);
          } else {
            ctx.reply("Something went wrong!");
          }
          console.log(e);
        }
      }
      return;
    }
    ctx.reply("Unknown command, /help to view available commands.");
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
  deviceId?: string
} & ExtraReplyMessage;

export async function notifyUser(msg: string | FmtString, opts: UserAndMsgOpts) {
  if (!opts || !opts.chainId && !opts.address && !opts.deviceId) {
    return;
  }
  const queryRes = await db.query(`
    SELECT DISTINCT tg_id FROM device2noti
    INNER JOIN address2device
    ON address2device.device_id = device2noti.device_id
    WHERE (address2device.chainid = $1 AND address2device.address = $2) OR (address2device.device_id = $3)) 
  `, [opts.chainId, opts.address, opts.deviceId]);
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

