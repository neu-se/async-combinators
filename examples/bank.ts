/**
 * A concurrency-safe bank using ReentrantAsyncLock, one lock per account.
 *
 * A shared balance with an async write step (simulating persistence) has no
 * built-in concurrency control: reading a balance, yielding, then writing it
 * back loses updates when calls overlap. The Bank fixes this with one
 * ReentrantAsyncLock per account: concurrent operations on the same account
 * are serialized, while operations on different accounts run in parallel.
 *
 * transfer acquires both account locks upfront. To prevent deadlock between
 * concurrent transfers in opposite directions, it always acquires them in
 * sorted-ID order. Reentrancy then lets withdraw and deposit re-enter their
 * account lock from within the transfer's critical section without blocking.
 *
 * Run:  npm run example:bank
 */
import { ReentrantAsyncLock } from '../src';
import assert from 'node:assert/strict';

type Balances = Record<string, number>;
const delay = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

// The obvious way to write it: read, yield, write back. Correct in isolation,
// but concurrent calls may observe a stale balance or lose updates.
async function naiveDeposit(balances: Balances, id: string, amount: number): Promise<void> {
  const balance = balances[id] ?? 0;
  await delay(10); // yield — concurrent calls read the same stale balance
  balances[id] = balance + amount;
}

// Per-account state: a balance and a reentrant lock that serializes all
// operations on that account.
interface Account {
  balance: number;
  readonly lock: ReentrantAsyncLock;
}

class Bank {
  private readonly accounts = new Map<string, Account>();

  private account(id: string): Account {
    if (!this.accounts.has(id))
      this.accounts.set(id, { balance: 0, lock: new ReentrantAsyncLock() });
    return this.accounts.get(id)!;
  }

  async getBalance(id: string): Promise<number> {
    return this.account(id).lock.runExclusive(async () => this.account(id).balance);
  }

  private async setBalance(id: string, value: number): Promise<void> {
    return this.account(id).lock.runExclusive(async () => {
      this.account(id).balance = value;
      await delay(5); // simulate async persistence
    });
  }

  async deposit(id: string, amount: number): Promise<void> {
    return this.account(id).lock.runExclusive(async () => {
      await this.setBalance(id, (await this.getBalance(id)) + amount);
    });
  }

  async withdraw(id: string, amount: number): Promise<void> {
    return this.account(id).lock.runExclusive(async () => {
      const balance = await this.getBalance(id);
      if (balance < amount) throw new Error(`account ${id}: insufficient funds`);
      await this.setBalance(id, balance - amount);
    });
  }

  // Acquires both account locks upfront in sorted-ID order so that two
  // concurrent transfers in opposite directions always contend in the same
  // order and cannot deadlock. Reentrancy lets the nested withdraw and deposit
  // re-enter their account lock from within this critical section.
  async transfer(from: string, to: string, amount: number): Promise<void> {
    const [firstId, secondId] = [from, to].sort();
    return this.account(firstId).lock.runExclusive(() =>
      this.account(secondId).lock.runExclusive(async () => {
        await this.withdraw(from, amount);
        await this.deposit(to, amount);
      })
    );
  }
}

async function main(): Promise<void> {
  const deposits = [10, 20, 30, 40]; // total 100

  // 1. Naive: concurrent deposits on the same account lose updates.
  const balances: Balances = {};
  await Promise.all(deposits.map((amt) => naiveDeposit(balances, 'alice', amt)));
  console.log(`naive:                      alice = ${balances['alice']}\t(expected 100)  <- updates lost`);
  assert.notEqual(balances['alice'], 100, 'naive deposits should lose updates');

  const bank = new Bank();

  // 2. Per-account lock: concurrent deposits on the same account are atomic.
  await Promise.all(deposits.map((amt) => bank.deposit('alice', amt)));
  await bank.deposit('bob', 100);
  console.log(`Bank (deposits):            alice = ${await bank.getBalance('alice')}\t(expected 100)  <- atomic`);

  // 3. Transfer: reentrant withdraw + deposit, locks acquired in sorted order.
  await bank.transfer('alice', 'bob', 25);
  console.log(`Bank (transfer):            alice = ${await bank.getBalance('alice')}\t(expected 75)   <- correct`);
  console.log(`Bank (transfer):            bob   = ${await bank.getBalance('bob')}\t(expected 125)  <- correct`);

  // 4. Concurrent transfers in opposite directions: sorted-order locking
  //    prevents the deadlock that would arise from naive two-lock acquisition.
  await Promise.all([bank.transfer('alice', 'bob', 10), bank.transfer('bob', 'alice', 10)]);
  console.log(`Bank (concurrent transfer): alice = ${await bank.getBalance('alice')}\t(expected 75)   <- no deadlock`);
  console.log(`Bank (concurrent transfer): bob   = ${await bank.getBalance('bob')}\t(expected 125)  <- no deadlock`);

  assert.equal(await bank.getBalance('alice'), 75);
  assert.equal(await bank.getBalance('bob'), 125);
  console.log('\nOK: the Bank is atomic and deadlock-free.');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
