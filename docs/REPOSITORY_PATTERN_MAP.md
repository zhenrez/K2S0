# Repository pattern and integration map

## 1. Selection method and evidence boundary

The configuration ranks a system, not isolated repositories. It selects a
small compatible spine while retaining the rest of the supplied stack as
adapters, optional engines, research sources, or consumers.

Evidence levels:

- **V1 inspected**: current public repository documentation inspected for this
  deliverable.
- **V2 packet**: mechanism is supported by the supplied K2S0 design packet.
- **U unverified**: repository retained, but no claim about its implementation
  is made without source inspection.

No third-party source code was copied. Repository patterns are reimplemented
behind original contracts. Before importing code, pin a commit and perform a
license, dependency, security, and code-level compatibility review.

## 2. Selected system spine

| Role | Selection | Disposition | Pattern harvested | Evidence |
| --- | --- | --- | --- | --- |
| Human data adapters | karlicoss/HPI | ADOPT PATTERN | Source ugliness ends in provider modules that yield common typed records | V1 |
| Ambient capture | screenpipe/screenpipe | ADAPTER | Event-driven capture, separate vision/audio services, local-first storage/API | V1 |
| Epistemic model | 8Dionysus/Dionysus + vlad-ds/selfos | ADOPT SEMANTICS | Evidence/claim separation, counterevidence, confidence dimensions, claim spine | V2 |
| Structural human model | Intuition-Lab/personal-model | ADOPT SEMANTICS | Evidence-linked Point → Line → Face → Volume → Root, correction/export | V1 + V2 |
| Elicitation | MakiDevelop/VirtualMe | ADOPT PATTERN | R1–R5 questioning, cross-session promotion, blind evaluation gates | V1 |
| Export readiness | SocioProphet/human-digital-twin | ADOPT PATTERN | Ω-style readiness, policy separate from scoring, minimized assurance export | V1 |
| Decision feedback | jayzalowitz/skytwin | ADOPT WITH BOUNDARY CHANGE | Domain-specific trust, typed preferences, explanations, feedback | V1 |
| Communication style | xming521/WeClone | OPTIONAL ISOLATED WORKER | Preserve conversational roles; optional style/voice projection | V1 |
| Durable entity runtime | restatedev/restate | PRIMARY ADAPTER | Per-entity consistent state, reliable calls, durable promises/timers | V1 |
| Long workflow alternative | temporalio/temporal | ALTERNATIVE, XOR | Replayable workflows and idempotent activities | V1 |
| Incremental derivation | cocoindex-io/cocoindex | ADAPTER | Declarative delta recomputation and content/code hashing | V1 |
| Tool/API federation | IBM/mcp-context-forge | EDGE GATEWAY | MCP/A2A/REST/gRPC federation, discovery, guardrails, observability | V1 |
| Untrusted plugin runtime | bytecodealliance/wasmtime | ADAPTER | WASI component sandbox with hard resources/capabilities | V1 |
| Secret isolation | Infisical/infisical + Infisical/agent-vault | ADAPTER | Agents receive proxied capability, not reusable raw credentials | V1 for agent-vault |
| Telemetry input | influxdata/telegraf | ADAPTER | Input/processor/aggregator/output plugin boundary | V1 |
| Governance | microsoft/agent-governance-toolkit | REFERENCE + ADAPTER | Deterministic tool policy, audit chain, outcome-verification gap awareness | V1 |
| Constraint solving | Z3Prover/z3 | OPTIONAL SERVICE | Mechanical invariant and satisfiability checks | retained |
| Optimization | Pyomo/pyomo + ERGO-Code/HiGHS | OPTIONAL SERVICE | Scenario/plan optimization below human constraints | retained |
| Causal analysis | py-why/dowhy | OPTIONAL RESEARCH WORKER | Causal claims get a distinct, assumption-explicit path | retained |

### Compatibility choices

- **Restate XOR Temporal** for the primary durable runtime. Running both in the
  same command path creates duplicate histories and unclear retry ownership.
  Restate is selected for entity-oriented twin state; Temporal remains the
  migration option for very long, operator-heavy workflows.
- **PostgreSQL AND object storage AND NATS JetStream** is the initial physical
  configuration: authoritative structured state, large private bytes, and
  low-latency distribution respectively.
- **Graph/vector indexes OPTIONAL**: they accelerate retrieval but cannot
  become the claim ledger.
- **Fine-tune/voice/avatar OPTIONAL AND downstream**: they consume compiled
  projections and never define identity.

## 3. Authoritative DT family: precise dispositions

| Repository | Role in this architecture | Not allowed to become |
| --- | --- | --- |
| SocioProphet/HolographMe | Avatar/likeness projection candidate; inspect before integration | legal identity or canonical person model |
| SocioProphet/human-digital-twin | Export-readiness and policy reference | complete twin ontology |
| xming521/WeClone | Chat-role-aware dataset and optional style worker; isolate pending AGPL review | canonical memory or unsupervised outbound agent |
| MakiDevelop/VirtualMe | Structured elicitation and validation adapter | sole evidence source |
| jayzalowitz/skytwin | Decision/explanation/feedback reference | combined knowledge-and-action authority |
| VenkataAnilKumar/SELPH | Retained U candidate; code-level role not asserted | core dependency before verification |
| 8Dionysus/Dionysus | Evidence/claim/adjudication semantic reference from packet | unreviewed direct code dependency |
| Intuition-Lab/personal-model | Structural and local-first model reference | one master persona |
| karlicoss/HPI | Historical data provider pattern | centralized canonical schema owner |
| screenpipe/screenpipe | Ambient capture producer | raw evidence authority outside Bronze |
| vlad-ds/selfos | Claim-ledger semantic reference from packet | vector-first identity store |
| huytieu/COG-second-brain | Human-readable Markdown/Git read model and skill surface | authoritative event/claim ledger |

## 4. Full supplied possibility space retained by family

The entries below are retained even when they are not on the first
implementation path. “Retained” means available for later progressive
evaluation, not implicitly approved.

| Family | Selected now | Retained / deferred candidates |
| --- | --- | --- |
| Agent memory/context | HPI-style adapters; optional Mem0 retrieval | mem0ai/mem0, xMannixx/agent-memory-skill, stephenschoettler/hermes-lcm, satellitecomponent/Neurite, zefhub/zef, memvid/memvid |
| Acquisition/retrieval | cocoindex-io/cocoindex | MODSetter/SurfSense, ScrapeGraphAI/Scrapegraph-ai, getmaxun/maxun, spider-rs/spider |
| Knowledge representation | ARGOCell/event/claim model | opencog/atomspace, Graphify-Labs/graphify, gyorilab/indra, indra_db, indra_db_lite, indra_cogex, indra_agent |
| Knowledge evolution | event-driven recomputation | pingchesu/hermes-curator-evolver, thakshak/ReasoningBank |
| Reasoning/inference | Z3 + policy + existing MARC/CHIP contracts | SoarGroup/Soar, human-avatar/skills-for-humanity, unimaginative-artist/SOMA, crbazevedo/reasongraph, giancarloerra/SocratiCodee, Swapnil-bo/FossilAI, clduab11/thinkrank, 5ynthaire/5YN-AbstractReasoning-LLM-Enhancement, Cornfy/LLM-Cognitive-Forge, KOSASIH/aetherion-os, cvmijg/eMage-, munch2u-a11y/Helix-AGI, Starlight143/crucible |
| Continuous reasoning | No core dependency | SakanaAI/continuous-thought-machines |
| Planning/gap finding | Host agent port | bpmsg/ahp-os, Taoidle/plan-cascade, AkoliteZA/hermes-agent-idea-workflow, Neeeophytee/finding-unknowns-skills |
| Research loops | Evaluation/model-worker plane | SkyworkAI/DeepResearchAgent, SakanaAI/AI-Scientist-v2, microsoft/RD-Agent, morluto/rea |
| Deliberation/debate | Optional review workers | datacendia/datacendia-core, slior/dialectic, Ayush-Kumar0207/The_Socratic_Arena |
| Solving/verification | Z3 optional; Pyomo + HiGHS optional | sktime/skpro, py-why/dowhy |
| Governance | Constitution + Microsoft toolkit reference | additional governance engines remain pluggable |
| Self-improvement | MorphIQ proposal/canary plane | SakanaAI/ShinkaEvolve, NousResearch/hermes-agent-self-evolution |
| Computer/device use | Downstream action adapters only | LvcidPsyche/auto-browser, fathah/hermes-desktop, raulvidis/hermes-android, open-jarvis/OpenJarvis, NousResearch/hermes-agent |
| Tool protocols | IBM ContextForge | 42-evey/hermes-plugins, zcaceres/fetch-mcp, grll/mcpadapt, CorsenAI/hermes-connector |
| Skills/code | Host capability plane | agentsmd/agents.md, Romanescu11/hermes-skill-factory, gakonst/nanocodex, KingLabsA/daedalus, SAP/leanix-self-built-software-agent, purepeepal/Agentic-Expert-System-for-Development, Hhhkarimi/repo2skill, shuyhere/repo-to-skill, shyamsridhar123/agentsmith-cli, SmallChX/repo-atlas, garrytan/gstack |
| Human interface | QuestN integration contract | adapt-ux/neuro-ux-sdk, nesquena/hermes-webui, BrainoutputHQ/brainoutput-community, anokye-labs/watchtower |
| Worlds/spatial | Wausauk33 adapter boundary | criptogus/HermesOffice, OneByJorah/VirtOffice, Lynpoint/CyberVerse, Tencent-Hunyuan/HunyuanWorld-1.0, nirholas/three.ws, kevtoe/worldview, matrixhub-ai/hfd |
| Avatar/media | Projection consumers | parthubhe/Agentic_MetaHumans, calesthio/OpenMontage, livekit/agents, microsoft/OmniParser |
| Agent runtime | Existing MARC-1 port; Restate below it | vstorm-co/pydantic-deepagents, omnigent-ai/omnigent, TheAiSingularity/hermesclaw, Cranot/super-Hermes, allenai/lumos, paperclipai/paperclip, NVIDIA/NemoClaw, Intelligent-Internet/zenith |
| Multi-agent | Host orchestration; no twin-core dependency | camel-ai/camel, Kocoro-lab/Shannon, vstorm-co/subagents-pydantic-ai, ruvnet/ruflo, shepherd-agents/shepherd, desplega-ai/agent-swarm |
| Durable execution | restatedev/restate XOR temporalio/temporal | both remain supported; only one owns a workflow |
| Scheduling/progress | Long-running workflow adapters | agentralabs/agentic-time, BradGroux/veritas-kanban |
| Monitoring/automation | Telegraf/OpenTelemetry; Restate | keephq/keep, n8n-io/n8n, VasiHemanth/tokentelemetry |
| Security | Infisical, agent-vault, Wasmtime, identity protocol | yv1ing/Z3r0, prompt-security/clawsec |
| API/model gateway | IBM ContextForge at agent edge | Portkey-AI/gateway at model edge |
| Communications | Notification adapter only | stalwartlabs/stalwart |
| Identity/trust | openagentidentityprotocol/agentidentityprotocol | compatible identity providers via adapter |
| Autonomous business | Consumers outside K2S0 | 1mancompany/OneManCompany, takaven/aidan-venture-os, aviskaar/open-org, itsPremkumar/Hermes-Full-Autonomous-Company, nicepkg/auto-company, zwbao/hermes-cofounder |
| Finance/markets | Restricted domain adapters and simulations | JerBouma/FinanceDatabase, microsoft/qlib-server, ccxt/ccxt, jesse-ai/jesse, TauricResearch/TradingAgents |
| Revenue/payment | Restricted action consumers | Garrettc123/autonomous-income-deployment, Abelhubprog/open-agent-wallet |
| Repo/business utilities | No twin-core dependency | MelvinJoshua1375/auto-dock-it, jpwinans/the-loom, Auro-rium/simulation-agent, Abdulaziz-almoshen/orbit, halfwavestudios/-MetaPrompts-Lab, microsoft/PromptWizard, veritasfuji-japan/veritas_os |

## 5. Exclusions from the canonical kernel

These are architectural exclusions, not corpus deletion:

- any fine-tuned model as canonical identity;
- any single vector or graph store as truth;
- any avatar/voice representation as principal identity;
- any agent runtime as direct K2S0 mutator;
- any business/trading/payment system inside the person-modeling trust zone;
- simultaneous Restate and Temporal ownership of the same workflow;
- direct code integration before commit pinning and license/security review.

## 6. Evidence that could reverse selections

The initial configuration should be reopened if:

- the undisclosed host repository already standardizes a different event bus,
  durable runtime, policy engine, schema registry, or database;
- an internal “dt” repository family exists and defines conflicting canonical
  contracts;
- measured latency shows PostgreSQL/outbox is insufficient for the hot path;
- Restate cannot satisfy replay, operational, or ecosystem requirements;
- a retained knowledge engine demonstrably preserves provenance, bitemporal
  state, contradiction, deletion propagation, and policy more completely than
  the current ARGOCell/event design;
- license review blocks a selected direct dependency.

