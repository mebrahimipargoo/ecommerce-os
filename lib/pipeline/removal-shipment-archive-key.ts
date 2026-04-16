/**
 * Cross-upload business identity for `amazon_removal_shipments` archive lines.
 * Aligns with the former `uq_amazon_removal_shipments_business_line` columns plus * carrier + shipment_date (shipment-detail semantics).
 */

import {
  normalizeRemovalOrderDateForBusinessKey,
  normalizeRemovalOrderQtyForBusinessKey,
  normalizeRemovalOrderUuidForBusinessKey,
  pgTextUniqueField,
} from "./amazon-removals-business-key";

/** Deterministic JSON key for "same logical shipment line" across different uploads. */
export function removalShipmentArchiveBusinessKey(row: Record<string, unknown>): string | null {
  const org = normalizeRemovalOrderUuidForBusinessKey(row.organization_id);
  const store = normalizeRemovalOrderUuidForBusinessKey(row.store_id);
  if (!org || !store) return null;
  const order_id = pgTextUniqueField(row.order_id);
  const tracking_number = pgTextUniqueField(row.tracking_number);
  if (!order_id && !tracking_number) return null;

  const tuple = {
    organization_id: org,
    store_id: store,
    order_id,
    tracking_number,
    sku: pgTextUniqueField(row.sku),
    fnsku: pgTextUniqueField(row.fnsku),
    disposition: pgTextUniqueField(row.disposition),
    requested_quantity: normalizeRemovalOrderQtyForBusinessKey(row.requested_quantity),
    shipped_quantity: normalizeRemovalOrderQtyForBusinessKey(row.shipped_quantity),
    disposed_quantity: normalizeRemovalOrderQtyForBusinessKey(row.disposed_quantity),
    cancelled_quantity: normalizeRemovalOrderQtyForBusinessKey(row.cancelled_quantity),
    order_date: normalizeRemovalOrderDateForBusinessKey(row.order_date),
    order_type: pgTextUniqueField(row.order_type),
    carrier: pgTextUniqueField(row.carrier),
    shipment_date: normalizeRemovalOrderDateForBusinessKey(row.shipment_date),
  };
  return JSON.stringify(tuple);
}
