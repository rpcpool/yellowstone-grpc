// Token instruction types
const TOKEN_INSTRUCTION_TYPES = {
    0: 'initializeMint',
    1: 'initializeAccount',
    2: 'initializeMultisig',
    3: 'transfer',
    4: 'approve',
    5: 'revoke',
    6: 'setAuthority',
    7: 'mintTo',
    8: 'burn',
    9: 'closeAccount',
    10: 'freezeAccount',
    11: 'thawAccount',
    12: 'transferChecked',
    13: 'approveChecked',
    14: 'mintToChecked',
    15: 'burnChecked',
  } as const;

  export function decodeTokenInstruction(data: Uint8Array, accountKeys: string[]) {
    // Convert Uint8Array to Buffer
    const buffer = Buffer.from(data);
    const instructionType = buffer[0];
    const type = TOKEN_INSTRUCTION_TYPES[instructionType];

    switch (instructionType) {
        case 7: { // mintTo
            const amount = buffer.slice(1, 9).readBigUInt64LE();
            return {
                type: 'mintTo',
                info: {
                    mint: accountKeys[0],
                    account: accountKeys[1],
                    mintAuthority: accountKeys[2],
                    amount: amount.toString()
                }
            };
        }
        default:
            return {
                type: type || 'unknown',
                data: buffer
            };
    }
  }
