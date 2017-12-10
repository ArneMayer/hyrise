namespace opossum {

BTreeIndex::BTreeIndex(const std::vector<std::shared_ptr<const BaseColumn>>& index_columns) {
}

Iterator BTreeIndex::_lower_bound(const std::vector<AllTypeVariant>& values) const {
  return std::vector().begin();
}

Iterator BTreeIndex::_upper_bound(const std::vector<AllTypeVariant>& values) const {
  return std::vector().begin();
}

Iterator BTreeIndex::_cbegin() const {
  return std::vector().begin();
}

Iterator BTreeIndex::_cend() const {
  return std::vector().begin();
}

} // namespace opossum