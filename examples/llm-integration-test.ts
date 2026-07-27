/**
 * Integration-test fixture pattern with withRecordReplay.
 *
 * LLM APIs are nondeterministic: the same prompt yields different text on each
 * call. That makes integration tests brittle — they can fail for reasons
 * unrelated to the code under test, and cannot run without a live API key.
 *
 * withRecordReplay provides a solution: record real API responses once into JSON fixture
 * files, then replay them in CI — fast, deterministic, no external dependency.
 *
 * Run:  npm run example:llm-test
 */
import path from 'node:path';
import assert from 'node:assert/strict';
import { withRecordReplay } from '../src';

// ---------- simulated LLM API ----------

interface ClassifyResponse {
  label: 'positive' | 'negative' | 'neutral';
  confidence: number;
}

// Returns a different (random) answer on every call — just like a real LLM.
async function callLLM(prompt: string): Promise<ClassifyResponse> {
  const labels = ['positive', 'negative', 'neutral'] as const;
  return {
    label: labels[Math.floor(Math.random() * labels.length)],
    confidence: Math.round((0.7 + Math.random() * 0.3) * 100) / 100,
  };
}

// ---------- tool under test ----------

// Classifies the sentiment of a text by delegating to the LLM API.
// Accepts the LLM caller as a parameter so tests can inject a recording wrapper.
async function classifySentiment(
  llm: (prompt: string) => Promise<ClassifyResponse>,
  text: string,
): Promise<ClassifyResponse> {
  return llm(`Classify the sentiment of: "${text}"`);
}

// ---------- demo ----------

// Fixture directory. In a real project, commit this directory to source control
// so CI can replay without a live API. Re-record with: RECORD=1 npm run llm-test
const FIXTURES_DIR = path.resolve(process.cwd(), 'examples', 'fixtures', 'llm-classifier');

const CASES = [
  'I love this product, it works great!',
  'Terrible experience, would not recommend.',
  'It is fine, nothing special.',
];

async function main(): Promise<void> {
  // 1. Raw: same input, two calls, different results each time. Any test that
  //    asserts on the output would be inherently flaky.
  console.log('1. Raw (nondeterministic) — two calls to the same input:');
  for (const text of CASES) {
    const a = await classifySentiment(callLLM, text);
    const b = await classifySentiment(callLLM, text);
    const differ = a.label !== b.label || a.confidence !== b.confidence;
    console.log(`   "${text}"`);
    console.log(`     call 1: ${JSON.stringify(a)}`);
    console.log(`     call 2: ${JSON.stringify(b)}  ${differ ? '<-- different' : ''}`);
  }

  // 2. Record: wrap callLLM so every call is captured to disk. The 'record' mode
  //    clears the fixture directory and writes a fresh snapshot. Use
  //    'incrementalRecord' (via RECORD=1) to fill in only new interactions
  //    without discarding existing ones.
  //
  //    Typical test-setup wiring:
  //      const mode = process.env.RECORD ? 'incrementalRecord' : 'replay';
  //      const recordedLLM = withRecordReplay(callLLM, FIXTURES_DIR, { mode });
  const recordedLLM = withRecordReplay(callLLM, FIXTURES_DIR, { mode: 'record' });
  console.log('\n2. Record — capturing fixtures for all test inputs:');
  for (const text of CASES) {
    const result = await classifySentiment(recordedLLM, text);
    console.log(`   recorded: "${text}" -> ${JSON.stringify(result)}`);
  }

  // 3. Replay: same fixture directory, replay mode. Every call returns the exact
  //    recorded response — fast, deterministic, and free of any live API call.
  //    An unrecorded interaction throws RecordingNotFoundError instead of
  //    silently calling the real API, so gaps never go unnoticed.
  const replayLLM = withRecordReplay(callLLM, FIXTURES_DIR, { mode: 'replay' });
  console.log('\n3. Replay (deterministic) — two calls to the same input:');
  for (const text of CASES) {
    const a = await classifySentiment(replayLLM, text);
    const b = await classifySentiment(replayLLM, text);
    assert.deepEqual(a, b, `replay must return the same result for "${text}"`);
    console.log(`   "${text}" -> ${JSON.stringify(a)}  (both calls identical)`);
  }

  console.log('\nOK: withRecordReplay makes nondeterministic LLM calls deterministic and CI-safe.');
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
