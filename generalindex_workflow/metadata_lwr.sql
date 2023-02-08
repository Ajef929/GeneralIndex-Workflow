ALTER TABLE metadata_recent ADD title_lwr varchar;
ALTER TABLE metadata_recent ADD author_lwr varchar;
ALTER TABLE metadata_recent ADD journal_lwr varchar;
UPDATE metadata_recent SET title_lwr = LOWER(title);
UPDATE metadata_recent SET author_lwr = LOWER(author);
UPDATE metadata_recent SET journal = LOWER(journal);
