import { createReadStream } from "node:fs";
import { Readable } from "node:stream";
import { CarReader } from "@ipld/car";
import { derive } from "@ucanto/principal/ed25519";
import { importDAG } from "@ucanto/core/delegation";
import { AgentData } from "@web3-storage/access";
import { Client } from "@web3-storage/w3up-client";
import commandLineArgs from "command-line-args";

// Default driver stores config to filesystem, we use an in-memory store
let storeInfo;
const store = {
  async open() { },
  async close() { },
  async reset() { },
  async save(data) { storeInfo = data; },
  async load() { return storeInfo; },
};

const optionDefinitions = [
  { name: 'path', type: String },
  { name: 'secret', type: String },
  { name: 'delegation', type: String },
];
const { path, secret, delegation } = commandLineArgs(optionDefinitions);

(async () => {
  try {
    const principal = await derive(Buffer.from(secret, "base64"));
    const lpAgentData = await AgentData.create({ principal }, { store });
    const lpClient = new Client(lpAgentData);

    const blocks = [];
    const reader = await CarReader.fromBytes(Buffer.from(delegation, "base64"));
    for await (const block of reader.blocks()) {
      blocks.push(block);
    }
    const proof = importDAG(blocks);

    const space = await lpClient.addSpace(proof);
    await lpClient.setCurrentSpace(space.did());

    const stream = () => Readable.toWeb(createReadStream(path));
    const link = await lpClient.uploadFile({ stream });

    process.stdout.write(link.toString());
    process.exitCode = 0;
  } catch (e) {
    process.stderr.write(e.toString());
    process.exitCode = 1;
  }
})();