#include <map>
#include <upcxx/upcxx.hpp>

class DistrMap
{
private:
    // store the local unordered map in a distributed object to access from RPCs
    using dobj_map_t = upcxx::dist_object<std::unordered_map<std::string, std::string>>;
    dobj_map_t local_map;
    // map the key to a target process
    int get_target_rank(const std::string &key)
    {
        return std::hash<std::string>{}(key) % upcxx::rank_n();
    }

public:
    // initialize the local map
    DistrMap() : local_map({}) {}

    // insert a key-value pair into the hash table
    upcxx::future<> insert(const std::string &key, const std::string &val)
    {
        // the RPC returns an empty upcxx::future by default
        return upcxx::rpc(get_target_rank(key),
                          // lambda to insert the key-value pair
                          [](dobj_map_t &lmap, std::string key, std::string val) {
                              // insert into the local map at the target
                              lmap->insert({key, val});
                          },
                          local_map, key, val);
    }
    
    // find a key and return associated value in a future
    upcxx::future<std::string> find(const std::string &key)
    {
        return upcxx::rpc(get_target_rank(key),
                          // lambda to find the key in the local map
                          [](dobj_map_t &lmap, std::string key) -> std::string {
                              auto elem = lmap->find(key);
                              // no key found
                              if (elem == lmap->end())
                                  return std::string();
                              // the key was found, return the value
                              return elem->second;
                          },
                          local_map, key);
    }
};
