
namespace opossum {

template <typename T>
class IntervalTreeNode {
 public:
   std::pair<T, T> get_interval();
 private:
  T _lower_bound;
  T _upper_bound
  

};

} // namespace opossum
