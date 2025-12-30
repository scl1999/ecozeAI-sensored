Enterprise Autonomous Agent Orchestrator (ecozeAI Core)
Status: Production (Pilot Phase with Big 4 Consultancies)
Stack: Node.js, Python, Google Cloud Platform (GCP), Vertex AI, OpenAI, Firebase.

Overview
This repository contains a sanitised snapshot of the core architecture behind ecozeAI, an autonomous supply chain analysis engine.

Unlike standard "chatbot" wrappers, this system is a headless, event-driven agentic infrastructure designed to perform "Deep Research" tasks without human intervention. It currently powers sustainability audits for enterprise clients (including KPMG and Capgemini), automating the creation of ISO-compliant Product Carbon Footprints (PCFs).

What This Code Demonstrates
This codebase is an example of "Production-Grade AI". It solves the hard problems that stop AI demos from becoming enterprise products:

Self-Healing Resilience: It doesn't crash when APIs fail. Custom retry logic with exponential backoff handles rate limits (429) and service outages (500) gracefully.
Cost Governance (FinOps): It tracks token usage in real-time. The system utilises Model Cascading—dynamically downgrading from expensive reasoning models (e.g., Gemini 1.5 Pro) to faster models (Flash) based on task complexity to protect margins.
Auditability & Provenance: In regulated industries, AI cannot hallucinate. This architecture traces every generated fact back to a specific source URL, saving the evidence trail to Firestore for audit compliance.
Polyglot Architecture: Demonstrates orchestration in Node.js (for scalable, serverless event handling) and specialised agent logic in Python (for data-heavy reasoning).

Repository Structure

File	Description
The Node.js/GCP Cloud Functions backend. Handles the "Master Agent" logic, Cloud Task queueing, Firestore state management, and the multi-turn chat loops that drive the research process.
deep_research_agent.py	The Python Specialist Agent. A focused reasoning agent demonstrating advanced prompt engineering, structured output parsing, and specific data analysis tasks best suited for the Python ecosystem.

-----

Key Architectural Patterns

1. The "Deep Research" Loop (orchestrator.js)
The system uses a recursive function calling loop to navigate the web. It doesn't just "search"; it plans, executes, reads, and refines.

See: apcfSupplierFinder (Deep Research Agent).
See: runGeminiStreamBrowserUse (The orchestration loop).
Logic: It autonomously navigates URLs, parses unstructured PDFs (via Tika), and cross-references data against a "Fact Check" agent before committing to the database.

-----

2. Governance & FinOps Layer
AI at scale is expensive. This system implements strict controls.

See: logAITransaction and calculateCost.
Logic: Every interaction is logged with exact input/output token costs. If a task exceeds a budget threshold, the system can autonomously terminate the branch or switch to a cheaper model.

-----

3. The "Hallucination Firewall"
To serve enterprise clients, "truth" is non-negotiable.

See: annotateAndCollectSources and saveURLs.
Logic: The AI is forced to cite its sources. A secondary "Reviewer Agent" parses the citations to ensure the URL actually contains the claimed data. If the URL is dead or irrelevant, the fact is discarded.
