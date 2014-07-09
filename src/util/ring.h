#ifndef CONSISTENT_H
#define CONSISTENT_H

#include <map>
#ifdef __GNUC__
#include <ext/hash_map>
#define HASH_NAMESPACE __gnu_cxx
#else
#include <hash_map>
#define HASH_NAMESPACE std
#endif
#include <string>
#include <sstream>

#include "common/base/FurcHash.h"
// http://www.martinbroadhurst.com/source/consistent.h.html


namespace Consistent
{

class EmptyRingException
{
};

template <class Data>
class Ring
{
public:
    typedef std::map<uint64_t, std::string> NodeMap;

    Ring(unsigned int replicas) : replicas_(replicas) {
    }

    uint64_t AddNode(const Node& node);
    void RemoveNode(const Node& node);
    const Node& GetNode(const Data& data) const;

private:
    NodeMap ring_;
    const unsigned int replicas_;
};

template <class Data>
uint64_t Ring<Data>::AddNode(const std::string& nodestr)
{
    uint64_t hash;
    for (unsigned int r = 0; r < replicas_; r++) {
        hash = furcHash((nodestr + Stringify(r)).c_str());
        ring_[hash] = node;
    }
    return hash;
}

template <class Data>
void Ring<Data>::RemoveNode(const std::string& nodestr)
{
    for (unsigned int r = 0; r < replicas_; r++) {
        uint64_t hash = furcHash((nodestr + Stringify(r)).c_str());
        ring_.erase(hash);
    }
}

template <class Data>
const Node& Ring<Data>::GetNode(const Data& data) const
{
    if (ring_.empty()) {
        throw EmptyRingException();
    }
    uint64_t hash = furcHash(Stringify(data).c_str());
    typename NodeMap::const_iterator it;
    // Look for the first node >= hash
    it = ring_.lower_bound(hash);
    if (it == ring_.end()) {
      // Wrapped around; get the first node
      it = ring_.begin();
    }
    return it->second;
}
    
}
