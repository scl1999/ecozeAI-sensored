import logging
import os

# Configure logging immediately
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Enable telemetry for Cloud Trace visibility in Agent Engine dashboard
os.environ["GOOGLE_CLOUD_AGENT_ENGINE_ENABLE_TELEMETRY"] = "true"
# Do not disable OTEL SDK - needed for --trace_to_cloud to work
# If SSL errors return, we'll handle them differently

os.environ.setdefault("GOOGLE_CLOUD_PROJECT", "ecoze-f216c")
os.environ.setdefault("GOOGLE_CLOUD_LOCATION", "global")
os.environ.setdefault("GOOGLE_GENAI_USE_VERTEXAI", "True")
# Set location to global to support gemini-3-pro-preview via global endpoint
# os.environ["GOOGLE_CLOUD_LOCATION"] = "global"

from functools import cached_property
from google.genai import Client
from google.adk.agents import LlmAgent
from google.adk.tools import agent_tool
from google.genai.types import ThinkingConfig, GenerateContentConfig, HttpOptions
from google.adk.planners import BuiltInPlanner
from google.adk.models.google_llm import Gemini

# Import built-in tools
from google.adk.tools.google_search_tool import GoogleSearchTool
from google.adk.tools import url_context


class GlobalGemini(Gemini):
    @cached_property
    def api_client(self) -> Client:
        http_options_kwargs = {"headers": self._tracking_headers}
        if self.retry_options:
            http_options_kwargs["retry_options"] = self.retry_options

        return Client(
            location="global", http_options=HttpOptions(**http_options_kwargs)
        )


# System instructions from apcfSupplierFinder (SYS_APCFSF)
SYS_APCFSF = """...
"""

fact_finder_google_search_agent = LlmAgent(
    name="fact_finder_google_search_agent",
    model=GlobalGemini(model="gemini-3-flash-preview"),
    description=("Agent specialized in performing Google searches."),
    sub_agents=[],
    instruction="Use the GoogleSearchTool to find information on the web.",
    tools=[GoogleSearchTool()],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_level="HIGH")
    ),
)
fact_finder_url_context_agent = LlmAgent(
    name="fact_finder_url_context_agent",
    model="gemini-2.5-flash",
    description=("Agent specialized in fetching content from URLs."),
    sub_agents=[],
    instruction="Use the UrlContextTool to retrieve content from provided URLs.",
    tools=[url_context],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_budget=24576)
    ),
)
fact_finder = LlmAgent(
    name="fact_finder",
    model=GlobalGemini(model="gemini-3-pro-preview"),
    description=(
        "Agent that handles a specific task, give it the exact prompt given to you by the user."
    ),
    sub_agents=[],
    instruction='You will be given a product name and its supply chain. This product may form part of other parent products which may also be given to you. Find the supplier / manufacturer / brand for the product (Product Name:).\nALWAYS use the provided tools to look up the supplier/manufacturer/brand. Only use the manufacturer / brand mentioned in the product name if you are sure the manufacturer / brand assembled and/or produced the product / material / component (PCMI) themselves. Do not use it as a fallback (e.g. Apple M4 system-on-chip (SoC) -> Supplier Name: Apple Inc. It could be -> Supplier Name: Taiwan Semiconductor Manufacturing Company (TSMC)), the answer may not be obvious.\n\n!! Where multiple suppliers are found for this PCMI for the exact end product, return the largest supplier to the parent product\'s manufacturer and the other suppliers separately..!!\n!! If you aren\'t given the supply chain for the product / material / component / ingredient, this is because the product is an end product. The supplier name will be the name of the end product manufacturer (/ brand if the brand and manufacturer are the same) !! \n!! As part of the supply chain you might have been given the supplier names and addresses of each parent PCMI - this is there in case the supplier we are looking for is dependent on the parent PCMI being location specific !!\n!! You must use your google_search and url_context tools to research the internet for facts, to find all the information. !!\n!! Always start with Google Search to find authoritative pages. For any promising result call urlContext to read the full content.!!\n!! SUPPLIER PROOF REQUIRED: Do not name a supplier without evidence of a direct buyer→supplier link. (Resort to strong evidence as a last resort) !!\n!!BAN MARKET SHARE INFERENCE: Never infer a supplier from “largest player,” market share, or capability alone!!\n!!DEFAULT UNKNOWN WHEN UNLINKED: If no qualifying link exists, set supplier_name: Unknown!!\n!! Don\'t use Wikepedia for your research unless it contains useful references !!\n!! Don\'t rush, take as long as you need. !!\n!! CRITICAL: If you use your search tool but find no direct evidence of a supplier link, you MUST return "*supplier_name: Unknown"". Do not guess. !!\n!! You must give sources and citations under every circumstance to back ALL of your claims.!!\n\nReturn your answer in the exact format shown below and return no other text:\n{if only one supplier found}\n*supplier_name: [name of supplier]\n*reasoning_supplier_identification: [a paragraph description of how you identified the supplier including any URLs for data sources]\n\n{if multiple suppliers found}\n*supplier_name: [name of the main / largest supplier]\n*reasoning_supplier_identification: [a short paragraph description of how you identified the supplier including any URLs for data sources and any snippets of text from these sources.]\n\n*other_supplier_1: [the name of the 1st other supplier]\n*reasoning_other_supplier_1: [a short paragraph description of how you identified the 1st other supplier including any URLs for data sources and any snippets of text from these sources.]\n\n...[repeat for all other suppliers]\n\n*other_supplier_N: [the name of the Nth other supplier]\n*reasoning_other_supplier_N: [a short paragraph description of how you identified the Nth other supplier including any URLs for data sources and any snippets of text from these sources.]\n\nIf you can\'t find the supplier, just return the string "Unknown" and no other text. You must try your hardest to find the actual supplier name. Set supplier_name to "Unknown" if you cant find it - do not estimate supplier name.\n\n--- Guidelines ---\nIf your own method of research fails, try the following:\n1.Authoritative disclosures (OEM/brand supplier lists, Form SD/CMR annexes, program-operator registries, govt product registries).\n2. Regulatory & corporate filings (national registers, DART/EDINET, Companies House, GLEIF LEI/OpenCorporates).\n3. Logistics & trade (bills of lading/manifest aggregators; shipment patterns matching production windows).\n4. Teardowns & component breadcrumbs (BOMs, part numbers, interface/certification databases).\n5. Standards, permits & site validation (ISO/IATF cert directories; emissions/permit registries).\n6. Geospatial & proximity (plant adjacency to upstream/downstream tiers; ports/rail).\n7. Commercial signals (press/JVs/awards/investor decks; public tenders/BoQs).\n8. People & capability clues (targeted job posts, account managers, talent exchange, commucations between companies and staff on social medias; R&D collabs/patents).\n\nHere are some extra details to help you on the above guidelines:\n\n1) Authoritative disclosures (highest weight)\n- OEM/brand supplier lists & CSR reports: Search for “Supplier List”, “Top Suppliers”, “Responsible Sourcing Report”. Extract legal entity names and categories (e.g., display/glass).\n- Conflict-minerals filings (Form SD/CMR): Parse smelter/refiner lists for upstream processors linked to the material/component; map to corporate parents.\n- SEC Filings and SEDAR Filings, and similar regulatory documents (SEC, SEDAR, ASX/ASIC, RNS, etc.)\n- Program-operator registries/certifications: EPD libraries, UL Product iQ, other certification databases often name manufacturers/sites; capture model/plant IDs.\n- Government product registries: EPREL (EU), CEC MAEDbS (US-CA), energy labels—pull manufacturer IDs, model variants, and sometimes plant info.\n2) Regulatory & corporate filings\n- National registers & securities filings: Korea DART, Japan EDINET, UK Companies House, US SEC/EDGAR: mine subsidiaries, manufacturing entities, JV announcements, related-party transactions.\n- Identity resolution: Use GLEIF LEI and OpenCorporates to unify aliases, prior names, addresses; align with shipping records and certificates.\n3) Logistics & trade evidence\n- Bills of lading/manifest aggregators: Query candidate supplier ⇄ tier-1 customer pairs. Look for repeated flows over months/years, matching the product’s production window.\n- HS code sanity checks: Validate that commodity codes and quantities align with the material/component (e.g., glass substrates vs. finished displays).\n4) Teardowns & component breadcrumbs\n- Independent teardowns/BOMs: Extract panel codes, controller ICs, module vendors; cross-reference with supplier catalogs.\n- Interface/certification databases: Bluetooth SIG QPL, USB-IF, Wi-Fi Alliance product finders—export CSV where possible to link modules ↔ vendors.\n5) Standards, permits & site validation\n- Management-system certificates: IAF/ISO directories (ISO 9001/14001/IATF 16949) to confirm factory scope and address; match to product family.\n- Industrial permits & emissions registries: EU IEP/E-PRTR, US EPA ECHO, local environmental portals—confirm process type (e.g., float glass line), capacity notes, geo-coordinates.\n6) Geospatial & proximity reasoning (corroboration only)\n- Plant adjacency: Check road/rail/port access; industrial park maps; reasonable trucking radius to upstream/downstream sites.\n- Satellite/OSM cues: Yard size, loading docks, rail spurs, tank farms—use to substantiate feasibility, not as sole proof.\n7) Commercial signals\n- Press/JV/awards/investor decks: Look for product-family mentions, supply awards, partnership statements naming categories (e.g., “glass for high-resolution displays”).\n- Public tenders & BoQs: EU TED, UK Contracts Finder, USASpending—award notices and annexes often list approved sub-suppliers/components.\n8) People & capability clues (supporting only)\n- Job postings/roles: “Key Account Manager for [OEM]”, “Supplier Quality Engineer onsite at [customer]” can signal active relationships.\n- R&D collabs & patents: Co-assigned patents, acknowledgments, or repeated citations connect firms in specific material processes.\n9) Spec-constrained capability matching (supporting only)\nWhen detailed product or component specifications are available (e.g. dimensions, performance ratings, tolerances, operating ranges, materials, finishing/coatings, certifications, IP ratings, safety classes, etc.), build a “spec fingerprint” and search for suppliers whose catalogues, data sheets or capability pages clearly show they can produce items with closely matching characteristics.\nConstruct this spec fingerprint as a structured bundle, for example:\n- Form factor & geometry: overall dimensions, thickness, aspect ratio, volume, shape constraints, mounting style.\n- Functional performance: capacity, power rating, speed, resolution, mechanical strength, thermal limits, electrical ratings, chemical resistance, durability cycles.\n- Materials & construction: base materials, alloys, polymers, composite types, surface treatments, coatings, laminations, adhesives.\n- Quality & compliance: standards and test methods (e.g. ASTM/ISO/EN), safety classes, IP ratings, flame ratings, medical/food-contact approvals.\n- Production constraints: minimum order quantities, typical batch sizes, process notes that match known production scales for the product.\nUse this fingerprint to:\n- Search supplier catalogues, line cards, capability matrices, and product configurators for items that match the spec cluster rather than a single attribute.\n- Prioritise suppliers that explicitly advertise series or families designed for that spec niche (e.g. “high-temperature adhesive for automotive electronics” or “ultra-thin flexible displays in the X-Y size range”).\n- Cross-check any candidate supplier against other evidence types (press releases, certifications, trade flows, teardowns) before treating them as a credible tier-1 or tier-N match.\n\n--- Additional Allowed Proof Sources (add to your toolkit) ---\n• Barcode/GS1 graphing: GTIN/UPC/Company Prefix via GEPIR/Verified by GS1 for brand-owner/legal entity and GLNs.\n• Certification registries: FCC ID; IECEE CB Scheme; IECEx; UL Product iQ; Wi-Fi Alliance; Bluetooth SIG; USB-IF; Wireless Power/other relevant consortia. Use certificate/file/ID numbers.\n• Regulatory product registries: EU EPREL and other official product databases where available.\n• Program-operator & ecolabel libraries that name manufacturers/sites.\n• Public procurement portals and award documentation that list approved sub-suppliers/components.\n• Entity-resolution backbones: GLEIF LEI, OpenCorporates to stitch trade, certification, and filings.\n\nPart A: Identifying the Supplier\n1.  **Direct Identification:** First, analyze the "Product Name" and "Product Chain" provided. The supplier or manufacturer may already be explicitly mentioned (e.g., "Corning Gorilla Glass" is made by "Corning").\n2.  **Official Brand Search:** For finished consumer goods (e.g., a phone, a car, a piece of clothing), the main brand is the supplier. Your goal is to find the official corporate name of that brand (e.g., for "Pixel 8" the supplier is "Google LLC").\n3.  **Component & Material Supplier Search:** For components, raw materials, or chemicals, the supplier will be a B2B company. Use targeted search queries like "[Product Name]" manufacturer", "who supplies "[Product Name]", or "[Product Name]" industrial supplier".',
    tools=[
        agent_tool.AgentTool(agent=fact_finder_google_search_agent),
        agent_tool.AgentTool(agent=fact_finder_url_context_agent),
    ],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_level="HIGH")
    ),
)
fact_checker_google_search_agent = LlmAgent(
    name="fact_checker_google_search_agent",
    model=GlobalGemini(model="gemini-3-flash-preview"),
    description=("Agent specialized in performing Google searches."),
    sub_agents=[],
    instruction="Use the GoogleSearchTool to find information on the web.",
    tools=[GoogleSearchTool()],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_level="HIGH")
    ),
)
fact_checker_url_context_agent = LlmAgent(
    name="fact_checker_url_context_agent",
    model="gemini-2.5-flash",
    description=("Agent specialized in fetching content from URLs."),
    sub_agents=[],
    instruction="Use the UrlContextTool to retrieve content from provided URLs.",
    tools=[url_context],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_budget=24576)
    ),
)
fact_checker_2 = LlmAgent(
    name="fact_checker_2",
    model=GlobalGemini(model="gemini-3-flash-preview"),
    description=("Fact checks the fact_finder AI agent. "),
    sub_agents=[],
    instruction='...',
    tools=[
        agent_tool.AgentTool(agent=fact_checker_google_search_agent),
        agent_tool.AgentTool(agent=fact_checker_url_context_agent),
    ],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_level="HIGH")
    ),
)

# Configure the root agent with specified parameters
root_agent = LlmAgent(
    name="supplier_finder",
    model=GlobalGemini(model="gemini-3-flash-preview"),
    description="Main Agent orchestrator for finding a supplier for a product / component / material",
    sub_agents=[fact_finder, fact_checker_2],
    instruction=SYS_APCFSF,
    tools=[],
    generate_content_config=GenerateContentConfig(
        temperature=1.0, max_output_tokens=65535
    ),
    planner=BuiltInPlanner(
        thinking_config=ThinkingConfig(include_thoughts=True, thinking_level="HIGH")
    ),
)

from google.adk.apps.app import App, ResumabilityConfig

# Use basic App wrapper - let Agent Engine use its default VertexAiSessionService
# Cloud Function will NOT pass session_id, letting each request be a new session
app = App(
    name="supplier_finder",
    root_agent=root_agent,
    # Set the resumability config to enable resumability.
    resumability_config=ResumabilityConfig(
        is_resumable=True,
    ),
)
