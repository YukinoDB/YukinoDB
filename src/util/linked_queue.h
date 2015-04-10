#ifndef YUKINO_UTIL_LINKED_QUEUE_H_
#define YUKINO_UTIL_LINKED_QUEUE_H_

namespace yukino {

namespace util {

// Double-Linked-List
struct Dll {

    template<class T>
    static inline void Init(T *x) {
        (x)->next = x;
        (x)->prev = x;
    }

    template<class T>
    static inline bool Empty(const T *x) {
        return (x)->next == x && (x)->prev == x;
    }

    template<class T>
    static inline void InsertHead(T *h, T *x) {
        (x)->next = (h)->next;
        (x)->next->prev = x;
        (x)->prev = h;
        (h)->next = x;
    }

    template<class T>
    static inline void InsertTail(T *h, T *x) {
        (x)->prev = (h)->prev;
        (x)->prev->next = x;
        (x)->next = h;
        (h)->prev = x;
    }

    template<class T>
    static inline void Remove(T *x) {
        (x)->next->prev = (x)->prev;
        (x)->prev->next = (x)->next;
    }

    template<class T>
    static inline int Count(T *h) {
        auto i = 0;
        for (auto entry = h->next; entry != h; entry = entry->next) {
            ++i;
        }
        return i;
    }

    template<class T> static inline T *Head(T *h) { return h->next; }

    template<class T> static inline T *Tail(T *h) { return h->prev; }

}; // struct Dll

} // namespace util

} // namespace yukino

#endif // YUKINO_UTIL_LINKED_QUEUE_H_