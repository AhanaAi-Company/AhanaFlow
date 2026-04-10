#!/usr/bin/env python3
"""
run_llm_memory_experiment.py
============================
Main experiment harness for testing LLM memory integration.

Runs baseline vs treatment comparison on locked conversation corpus.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
EVENT_STREAMS_DIR = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(EVENT_STREAMS_DIR) not in sys.path:
    sys.path.insert(0, str(EVENT_STREAMS_DIR))

from tools.acp_logging import get_logger
from llm_memory.client import MemoryClient
from llm_memory.retrieval import MemoryRetriever

log = get_logger("llm_memory_experiment")


def load_conversation_corpus(corpus_path: Path) -> dict:
    """Load locked conversation corpus."""
    log.info("loading_corpus", path=str(corpus_path))
    return json.loads(corpus_path.read_text())


def run_baseline_mode(
    conversation: dict,
    output_dir: Path,
) -> dict:
    """
    Run conversation in baseline mode (no memory retrieval).
    
    Returns result dict with outputs.
    """
    conversation_id = conversation["id"]
    topic = conversation.get("topic", "general")
    
    log.info("running_baseline", conversation=conversation_id, topic=topic)
    
    results = {
        "conversation_id": conversation_id,
        "mode": "baseline",
        "topic": topic,
        "turns": [],
    }
    
    # Simulate LLM generation for each turn
    # In real implementation, this would call actual LLM
    for turn in conversation["turns"]:
        turn_result = {
            "turn_index": turn["turn_index"],
            "user_prompt": turn["user_prompt"],
            "baseline_output": f"[BASELINE RESPONSE: {turn['user_prompt'][:50]}]",
            "latency_ms": 0,
        }
        results["turns"].append(turn_result)
    
    return results


def run_treatment_mode(
    conversation: dict,
    retriever: MemoryRetriever,
    output_dir: Path,
) -> dict:
    """
    Run conversation in treatment mode (with memory retrieval).
    
    Returns result dict with outputs and retrieved memories.
    """
    conversation_id = conversation["id"]
    topic = conversation.get("topic", "general")
    
    log.info("running_treatment", conversation=conversation_id, topic=topic)
    
    results = {
        "conversation_id": conversation_id,
        "mode": "treatment",
        "topic": topic,
        "turns": [],
    }
    
    for turn in conversation["turns"]:
        user_prompt = turn["user_prompt"]
        
        # Retrieve similar memories
        start_time = time.time()
        context, memories = retriever.retrieve_and_build_context(
            user_prompt=user_prompt,
            top_k=3,
            similarity_threshold=0.7,
        )
        retrieval_latency = (time.time() - start_time) * 1000
        
        # Simulate LLM generation with augmented context
        # In real implementation, this would call actual LLM with context
        treatment_output = f"[TREATMENT RESPONSE WITH {len(memories)} MEMORIES: {user_prompt[:50]}]"
        
        turn_result = {
            "turn_index": turn["turn_index"],
            "user_prompt": user_prompt,
            "treatment_output": treatment_output,
            "retrieved_memories": len(memories),
            "memory_scores": [m.score for m in memories],
            "latency_ms": retrieval_latency,
            "augmented_context": context if memories else None,
        }
        results["turns"].append(turn_result)
    
    return results


def populate_memory_bank(
    conversations: list[dict],
    retriever: MemoryRetriever,
) -> int:
    """
    Populate vector memory with conversation history.
    
    Returns number of turns stored.
    """
    log.info("populating_memory_bank", conversations=len(conversations))
    
    stored_count = 0
    
    for conv in conversations:
        conversation_id = conv["id"]
        topic_tags = [conv.get("topic", "general")]
        
        for turn in conv["turns"]:
            success = retriever.store_conversation_turn(
                conversation_id=conversation_id,
                turn_index=turn["turn_index"],
                user_prompt=turn["user_prompt"],
                assistant_response=turn.get("assistant_response", "[SIMULATED RESPONSE]"),
                topic_tags=topic_tags,
            )
            
            if success:
                stored_count += 1
    
    log.info("memory_bank_populated", stored=stored_count)
    return stored_count


def run_experiment(
    corpus_path: Path,
    output_dir: Path,
    memory_bank_size: int = 15,
) -> dict:
    """
    Run full experiment: baseline vs treatment on locked corpus.
    
    Args:
        corpus_path: Path to conversation corpus JSON
        output_dir: Output directory for results
        memory_bank_size: Number of conversations to use for memory bank
    
    Returns:
        Experiment report dict
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Load corpus
    corpus = load_conversation_corpus(corpus_path)
    all_conversations = corpus.get("conversations", [])
    
    if len(all_conversations) < memory_bank_size + 5:
        raise ValueError(
            f"Corpus has {len(all_conversations)} conversations, "
            f"need at least {memory_bank_size + 5} (15 for memory bank + 5 for test)"
        )
    
    # Split: first N for memory bank, remaining for test
    memory_conversations = all_conversations[:memory_bank_size]
    test_conversations = all_conversations[memory_bank_size:]
    
    log.info("experiment_split",
             memory_bank=len(memory_conversations),
             test_set=len(test_conversations))
    
    # Initialize memory retriever
    client = MemoryClient()
    
    # Test connection
    if not client.ping():
        raise RuntimeError("Vector server not responding. Start with: python -m vector_server.cli serve")
    
    # Create collection and build index
    client.create_collection(dimension=384)
    
    retriever = MemoryRetriever(client=client)
    
    # Populate memory bank
    stored = populate_memory_bank(memory_conversations, retriever)
    
    # Build HNSW index
    client.build_hnsw_index()
    
    # Run experiments
    baseline_results = []
    treatment_results = []
    
    for conv in test_conversations:
        # Baseline mode
        baseline_result = run_baseline_mode(conv, output_dir)
        baseline_results.append(baseline_result)
        
        # Treatment mode
        treatment_result = run_treatment_mode(conv, retriever, output_dir)
        treatment_results.append(treatment_result)
    
    # Build report
    report = {
        "artifact_version": 1,
        "experiment": "llm_conversational_memory",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "corpus": {
            "path": str(corpus_path),
            "total_conversations": len(all_conversations),
            "memory_bank_size": len(memory_conversations),
            "test_set_size": len(test_conversations),
            "stored_turns": stored,
        },
        "baseline_results": baseline_results,
        "treatment_results": treatment_results,
        "summary": {
            "total_test_conversations": len(test_conversations),
            "total_baseline_turns": sum(len(r["turns"]) for r in baseline_results),
            "total_treatment_turns": sum(len(r["turns"]) for r in treatment_results),
            "avg_memories_retrieved": sum(
                sum(t["retrieved_memories"] for t in r["turns"])
                for r in treatment_results
            ) / max(sum(len(r["turns"]) for r in treatment_results), 1),
            "avg_retrieval_latency_ms": sum(
                sum(t["latency_ms"] for t in r["turns"])
                for r in treatment_results
            ) / max(sum(len(r["turns"]) for r in treatment_results), 1),
        },
    }
    
    # Write report
    report_path = output_dir / "llm_memory_experiment_report.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    
    log.info("experiment_complete", report=str(report_path))
    
    return report


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run LLM memory integration experiment"
    )
    parser.add_argument(
        "--corpus",
        type=Path,
        required=True,
        help="Path to conversation corpus JSON",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("business_ecosystem/33_event_streams/reports"),
        help="Output directory for results",
    )
    parser.add_argument(
        "--memory-bank-size",
        type=int,
        default=15,
        help="Number of conversations for memory bank (default: 15)",
    )
    args = parser.parse_args()
    
    if not args.corpus.exists():
        print(f"ERROR: Corpus not found: {args.corpus}")
        sys.exit(1)
    
    try:
        report = run_experiment(
            corpus_path=args.corpus,
            output_dir=args.output,
            memory_bank_size=args.memory_bank_size,
        )
        
        print("\n" + "=" * 60)
        print("LLM MEMORY EXPERIMENT COMPLETE")
        print("=" * 60)
        print(f"Memory bank size: {report['corpus']['memory_bank_size']} conversations")
        print(f"Test set size: {report['corpus']['test_set_size']} conversations")
        print(f"Stored turns: {report['corpus']['stored_turns']}")
        print(f"Avg memories retrieved: {report['summary']['avg_memories_retrieved']:.2f}")
        print(f"Avg retrieval latency: {report['summary']['avg_retrieval_latency_ms']:.1f} ms")
        print(f"\nReport: {args.output / 'llm_memory_experiment_report.json'}")
        print("=" * 60)
        
    except Exception as e:
        log.error("experiment_failed", error=str(e))
        print(f"\nERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
