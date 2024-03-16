#!/usr/bin/env -S deno run -A --unstable-kv
// pmim-data-greatdict/tool/gen_db_sys_dict.js
// 生成 pmim_sys.db 数据库 (词库)
//
// 命令行示例:
// > cat pyim-greatdict.pyim \
// | deno run -A --unstable-kv gen_db_sys_dict.js pmim_sys.db
import { join } from "https://deno.land/std@0.217.0/path/join.ts";
import { TextLineStream } from "https://deno.land/std@0.217.0/streams/text_line_stream.ts";

import { batch_set, chunk_set } from "./kv_util.js";

async function 读取stdin() {
  const 行 = Deno.stdin.readable
    .pipeThrough(new TextDecoderStream())
    .pipeThrough(new TextLineStream());

  const o = [];
  for await (const i of 行) {
    // 忽略注释和空行
    if (i.startsWith(";")) {
      continue;
    }
    if (i.trim().length < 1) {
      continue;
    }
    o.push(i);
  }
  return o;
}

class 字频读取器 {
  constructor(kv) {
    this.kv = kv;
    this.cache = {};
  }

  async 初始化() {
    // 加载 preload/freq_tgh
    this.pt = await this.kv.get(["data", "preload", "freq_tgh"]);
  }

  // 获取汉字对应的频率
  async 频率(c) {
    if (null != this.pt[c]) {
      return this.pt[c];
    }

    if (null != this.cache[c]) {
      return this.cache[c];
    }

    const { value } = await this.kv.get(["data", "freq_d", c]);
    if (null != value) {
      this.cache[c] = value;
    } else {
      this.cache[c] = 0;
    }
    return this.cache[c];
  }
}

// 将字符串按照 unicode code point 切分成单个字符
export function u切分(s) {
  const o = [];
  let i = 0;
  while (i < s.length) {
    const c = s.codePointAt(i);
    o.push(String.fromCodePoint(c));
    if (c > 0xffff) {
      i += 2;
    } else {
      i += 1;
    }
  }
  return o;
}

class 词库收集器 {
  constructor(kv, p) {
    this.kv = kv;
    this.p = p;

    // 拼音至前缀
    this._pp = {};
    // 前缀至词
    this._pt = {};

    // 添加词的计数
    this._c = 0;
    // 丢弃计数
    this._cd = 0;
  }

  // 添加一个词条
  添加(拼音, 汉字) {
    const 字 = u切分(汉字);
    // 如果拼音个数和汉字个数不同, 丢弃
    if (拼音.length != 字.length) {
      console.log("  丢弃: " + 拼音 + " -> " + 汉字);
      this._cd += 1;
      return;
    }
    // 至少 2 个字
    if (拼音.length < 2) {
      console.log("  丢弃: " + 拼音 + " -> " + 汉字);
      this._cd += 1;
      return;
    }

    const pin_yin = 拼音.slice(0, 2).join("_");
    const 前缀 = 字.slice(0, 2).join("");

    // 拼音至前缀
    if (null == this._pp[pin_yin]) {
      this._pp[pin_yin] = [];
    }
    this._pp[pin_yin].push(前缀);
    // 前缀至词
    if (null == this._pt[前缀]) {
      this._pt[前缀] = [];
    }
    this._pt[前缀].push(汉字);

    this._c += 1;
  }

  // 将词库写入数据库
  async 保存() {
    console.log("丢弃: " + this._cd);
    console.log("总词数: " + this._c);

    // 拼音至前缀
    const 写入1 = [];
    for (const i of Object.keys(this._pp)) {
      // 去重, 排序
      const 前缀 = Array.from(new Set(this._pp[i]));
      前缀.sort();

      写入1.push([["data", "dict", i], 前缀]);
    }
    console.log("  拼音 -> 前缀: " + 写入1.length);
    await batch_set(this.kv, 写入1, 1000);

    // 前缀至词
    const 写入2 = [];
    // 超大值计数
    let 超大 = 0;
    for (const i of Object.keys(this._pt)) {
      // 去重, 排序
      const 词 = Array.from(new Set(this._pt[i]));
      词.sort();

      // 检查大小
      const 大小 = 计算大小(词);
      if (大小 > 65536) {
        超大 += 1;
        console.log("超大 (" + i + ") " + 词.length + ", " + 大小 + " 字节");
        // 采用 chunk 方式存储
        await this.kv.set(["data", "dict", i], "chunk");
        await chunk_set(this.kv, ["data", "dict", i], 词);
      } else {
        // 普通值
        写入2.push([["data", "dict", i], 词]);
      }
    }
    console.log("  前缀 -> 词 (超大): " + 超大);
    console.log("  前缀 -> 词: " + 写入2.length);
    await batch_set(this.kv, 写入2, 1000);
  }
}

function 转换数据(数据, 库) {
  // pyim 数据格式, 比如:
  // a-a 阿阿 啊啊 阿啊

  for (const i of 数据) {
    const p = i.split(" ");
    const 拼音 = p[0].split("-");

    // 添加每一个词条
    for (const j of p.slice(1)) {
      库.添加(拼音, j);
    }
  }
}

// deno-kv 存储的值占用的字节数
function 计算大小(v, n) {
  const t = JSON.stringify(v);
  const b = new TextEncoder().encode(t);
  return b.length;
}

async function 处理(kv, 数据, 库) {
  console.log("处理()");

  转换数据(数据, 库);

  console.log("保存 .. .");
  await 库.保存();

  console.log("写入元数据");
  const PMIM_DB_VERSION = "pmim_sys_db version 0.1.0";
  const PMIM_VERSION = "pmim version 0.1.6";

  await kv.set(["pmim_db", "v"], {
    pmim: PMIM_VERSION,
    deno_version: Deno.version,
    n: "胖喵拼音内置数据库 (332 万词, pyim-greatdict)",
    _last_update: new Date().toISOString(),
  });
  // 标记没有词的频率数据
  await kv.set(["pmim_db", "sys_dict_nc"], 1);
}

async function main() {
  const 输出 = Deno.args[0];
  console.log(`${输出}`);

  // 打开数据库
  const kv = await Deno.openKv(输出);

  const p = new 字频读取器(kv);
  await p.初始化();
  const 库 = new 词库收集器(kv, p);

  const 数据 = await 读取stdin();
  console.log("  输入数据条数: " + 数据.length);

  await 处理(kv, 数据, 库);

  // 记得关闭数据库
  kv.close();
}

if (import.meta.main) main();
