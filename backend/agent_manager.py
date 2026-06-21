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

PRODUCT_NAME = "Apple iPhone 14 Pro Max"
URLS = ["THIS JUST A TEST, REPOND WITH `ACKNOWLEDGED`", "THIS JUST A TEST, REPOND WITH `ACKNOWLEDGED`", "THIS JUST A TEST, REPOND WITH `ACKNOWLEDGED`", "THIS JUST A TEST, REPOND WITH `ACKNOWLEDGED`", "THIS JUST A TEST, REPOND WITH `ACKNOWLEDGED`"]
TARGET_BUDGET = 600
MAX_PRICE = 800

async def main():
    load_dotenv()  # loads your LLM provider key, e.g. ANTHROPIC_API_KEY

    adapter = AnthropicAdapter(
        model="claude-sonnet-4-6",
        custom_section=f"""You are the Lead Coordinator of a digital strike team comprising 5 specialized negotiation agents. Your objective is to secure the absolute lowest possible purchase price for a specific item on Facebook Marketplace by pitting multiple sellers against each other in real-time.
            Product to secure: {PRODUCT_NAME}
            Target Budget: ${TARGET_BUDGET}
            Absolute Max Price: ${MAX_PRICE} (Do not authorize any offers above this limit under any circumstances)

            Marketplace Listings:
            1. {URLS[0]}
            2. {URLS[1]}
            3. {URLS[2]}
            4. {URLS[3]}
            5. {URLS[4]}

            Your Operational Directives:
            0. In your communication with each worker, keep the messages as concise and straightforward as possible. NO EMOJI, NO FLOWERY LANGUAGE, NO EXCESSIVE PLEASANTRIES. The goal is to be clear and direct.
            1. Initialization: Immediately use the `thenvoi_create_chatroom` tool to create a new central workspace. You can name it "{PRODUCT_NAME} War Room".
            2. Team Assembly: Once the room is created, use the `thenvoi_lookup_peers` tool to search the directory for "worker1", "worker2", "worker3", "worker4", and "worker5". Once you have their IDs, use the `thenvoi_add_participant` tool to invite all 5 workers into the newly created room.
            3. Deployment: Assign each of your 5 workers to one of the specific listing URLs above. 
            4. Information Arbitrage: You are the central intelligence hub. You must actively monitor all 5 negotiations. If any worker successfully negotiates a lower price (e.g., Worker 1 gets a seller down from $100 to $50), you must immediately broadcast this new lowest price to the other 4 workers.
            5. Leverage: Instruct the remaining workers to use this new price as direct leverage in their ongoing chats. (e.g., "Tell your seller: 'I have another local seller willing to do $50 right now. If you can beat that and do $40, I will come pick it up from you instead.'")
            6. Escalation: Continue cycling this information arbitrage until all sellers reach their absolute bottom line or stop responding.
            7. Finalization: Once the lowest possible market floor is found, halt all workers. Verify the final price is below the Absolute Max Price, and present the single winning link and price to the user for final approval.""",
        enable_execution_reporting=True,
    )

    agent_id, api_key = load_agent_config("manager")
    agent = Agent.create(adapter=adapter, agent_id=agent_id, api_key=api_key)

    logger.info("Agent is running! Press Ctrl+C to stop.")
    await agent.run()  # opens a persistent WebSocket and listens forever

if __name__ == "__main__":
    asyncio.run(main())