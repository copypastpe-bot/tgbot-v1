ALTER TABLE cashbook_entries
ADD COLUMN IF NOT EXISTS category text;

CREATE INDEX IF NOT EXISTS idx_cashbook_entries_expense_category
ON cashbook_entries(category)
WHERE kind = 'expense' AND category IS NOT NULL;
