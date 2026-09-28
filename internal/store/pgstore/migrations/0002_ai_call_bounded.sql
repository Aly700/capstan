-- D32: only reservations with positive token bounds and a server price are bounded.
-- Existing rows and legacy writers retain their estimate-only accounting.
alter table ai_call add column bounded boolean not null default false;
