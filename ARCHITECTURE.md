## 🏛️ Master Architecture: Logistics & AI ERP
- **Paradigm:** Strict Object-Oriented (OOP) & Multi-tenant (RLS enforced).
- **Architecture:** Hybrid Microservices (Next.js for UI, Python FastAPI for AI/Marketplace).

## 🗄️ Data Strategy (5-Layer Engine)
1. **Operational (PostgreSQL):** Main DB for items, pallets, and claims.
2. **Landing Zone:** Using JSONB columns (`raw_data`) in `marketplace_orders` and `returns` for flexible API integration.
3. **Analytics (Planned):** Google BigQuery for Helium 10 style BI.
4. **Storage:** AWS S3 for all images/PDFs.
5. **Search:** Algolia/Elasticsearch for instant code lookup.

## 🏷️ Standard Identifier Rules (DO NOT CHANGE)
- Every item MUST use: `asin`, `fnsku`, `sku`.
- Hierarchy: Pallet (Grandparent) -> Package (Parent) -> Return Item (Child).

## 🤖 Python Agent & Live Data Bridge
- Python FastAPI handles all SP-API Amazon connections.
- It polls `claim_submissions` for `ready_to_send` status.
- It records all actions in `system_audit_logs` and `claim_history_logs`.

## 📜 Audit & History Policy
- Every status change, manual edit, or Agent action must be logged in `system_audit_logs`.
- We use Event Sourcing logic: Track what happened, when, and by whom.

## 🛠️ Module Roadmap
- Warehouse Scanner (Done)
- Claim Engine UI & PDF (In Progress)
- Python Agent / SP-API (Starting Tomorrow)
- D2C CRM & Advanced Analytics (Future)

Farsi Summary: این فایل را به عنوان نقشه راه دائمی سیستم ایجاد کن. از این پس تمام کدها باید با این ساختار هماهنگ باشند و از JSONB برای ذخیره دیتای لایو آمازون استفاده شود تا مهاجرت به پایتون بدون مشکل انجام شود.
