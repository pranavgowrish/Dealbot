import asyncio
import logging
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.memory import InMemorySaver
from thenvoi import Agent
from thenvoi.adapters import LangGraphAdapter, AnthropicAdapter
from thenvoi.config import load_agent_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Wrapper to automatically reconnect dropped WebSockets
async def run_with_reconnect(worker, name):
    while True:
        try:
            logger.info(f"[{name}] Connecting to Band...")
            await worker.run()
        except Exception as e:
            logger.warning(f"[{name}] Disconnected: {e}. Reconnecting in 3 seconds...")
            await asyncio.sleep(3)

async def main():
    load_dotenv()

    worker_names = ["worker1", "worker2", "worker3", "worker4", "worker5"]
    run_tasks = []

    for name in worker_names:
        # Create a UNIQUE adapter for each worker to establish their specific identity
        adapter = AnthropicAdapter(
            model="claude-sonnet-4-6",
            custom_section=f"""You are {name}, an autonomous negotiation agent operating on Facebook Marketplace. Your singular goal is to secure the absolute lowest possible purchase price for your assigned item. You are part of a coordinated digital strike team overseen by a Lead Coordinator.

                Your Operational Directives:
                0. In your communication with the Lead Coordinator, keep the messages as concise and straightforward as possible. NO EMOJI, NO FLOWERY LANGUAGE, NO EXCESSIVE PLEASANTRIES. The goal is to be clear and direct.
                1. Identity: You are explicitly {name}. Only respond to instructions directed at you or the general team by the Lead Coordinator.
                2. Independence: Manage your assigned seller interaction entirely independently.
                3. Act Human: Keep your messages to the seller short (1-2 sentences max). Real buyers do not write paragraphs.
                4. Team Leverage: The Lead Coordinator will periodically feed you lower prices achieved by your fellow workers. You must immediately use this as direct leverage against your seller (e.g., 'I have another local seller willing to do $[Price] right now. If you can beat that and do $[Lower Price], I will come pick it up from you instead.').
                5. Communication with Lead Coordinator: Only communicate with the Lead Coordinator, if you reach a new lowest price with your seller, or if your seller stops responding. Do not communicate with the Lead Coordinator for any other reason.
                6. Finalization: If you reach a final price with your seller that is below the Absolute Max Price, immediately report this to the Lead Coordinator and await further instructions.""",
            enable_execution_reporting=True,
        )

        agent_id, api_key = load_agent_config(name)
        worker = Agent.create(adapter=adapter, agent_id=agent_id, api_key=api_key)
        
        # Add the worker to the concurrent task list using the reconnect wrapper
        run_tasks.append(run_with_reconnect(worker, name))

    logger.info(f"{len(worker_names)} agents are starting! Press Ctrl+C to stop.")
    
    # Run all workers concurrently
    await asyncio.gather(*run_tasks)

if __name__ == "__main__":
    asyncio.run(main())