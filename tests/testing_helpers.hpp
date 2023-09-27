#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>

namespace message_queue::testing {
///
/// @brief Simple Container type. Can be used to test library function with containers other than vector.
///
template <typename T>
class OwnContainer {
public:
    using value_type = T;
    using iterator = T*;
    using const_iterator = T const*;

    OwnContainer() : OwnContainer(0) {}

    OwnContainer(size_t size) : OwnContainer(size, T{}) {}
    OwnContainer(size_t size, T value) : _data(nullptr), _size(size), _copy_count(std::make_shared<size_t>(0)) {
        _data = new T[_size];
        std::for_each(this->begin(), this->end(), [&value](T& val) { val = value; });
    }
    OwnContainer(std::initializer_list<T> elems)
        : _data(nullptr), _size(elems.size()), _copy_count(std::make_shared<size_t>(0)) {
        _data = new T[_size];
        std::copy(elems.begin(), elems.end(), _data);
    }
    OwnContainer(OwnContainer<T> const& rhs) : _data(nullptr), _size(rhs.size()), _copy_count(rhs._copy_count) {
        _data = new T[_size];
        std::copy(rhs.begin(), rhs.end(), _data);
        (*_copy_count)++;
    }
    OwnContainer(OwnContainer<T>&& rhs) : _data(rhs._data), _size(rhs._size), _copy_count(rhs._copy_count) {
        rhs._data = nullptr;
        rhs._size = 0;
        rhs._copy_count = std::make_shared<size_t>(0);
    }

    ~OwnContainer() {
        if (_data != nullptr) {
            delete[] _data;
            _data = nullptr;
        }
    }

    OwnContainer<T>& operator=(OwnContainer<T> const& rhs) {
        this->_data = new T[rhs._size];
        this->_size = rhs._size;
        std::copy(rhs.begin(), rhs.end(), _data);
        this->_copy_count = rhs._copy_count;
        (*_copy_count)++;
        return *this;
    }

    OwnContainer<T>& operator=(OwnContainer<T>&& rhs) {
        delete[] _data;
        _data = rhs._data;
        _size = rhs._size;
        _copy_count = rhs._copy_count;
        rhs._data = nullptr;
        rhs._size = 0;
        rhs._copy_count = std::make_shared<size_t>(0);
        return *this;
    }

    T* data() noexcept {
        return _data;
    }

    T const* data() const noexcept {
        return _data;
    }

    std::size_t size() const {
        return _size;
    }

    void resize(std::size_t new_size) {
        if (new_size <= this->size()) {
            _size = new_size;
            return;
        }
        T* new_data = new T[new_size];
        std::copy(this->begin(), this->end(), new_data);
        std::for_each(new_data + this->size(), new_data + new_size, [](T& val) { val = T{}; });
        _size = new_size;
        delete[] _data;
        _data = new_data;
    }

    T const& operator[](size_t i) const {
        return _data[i];
    }

    T& operator[](size_t i) {
        return _data[i];
    }

    size_t copy_count() const {
        return *_copy_count;
    }

    bool operator==(OwnContainer<T> const& other) const {
        if (other.size() != this->size()) {
            return false;
        }
        for (size_t i = 0; i < _size; i++) {
            if (!(other[i] == this->operator[](i))) {
                return false;
            }
        }
        return true;
    }

    bool operator!=(OwnContainer<T> const& other) const {
        return !(*this == other);
    }

    T* begin() const {
        return _data;
    }

    T* end() const {
        return _data + _size;
    }

    T const* cbegin() const {
        return _data;
    }

    T const* cend() const {
        return _data + _size;
    }

private:
    T* _data;
    size_t _size;
    std::shared_ptr<size_t> _copy_count;
};
template <typename T>
struct CustomAllocator {
    using value_type = T;
    using pointer = T*;
    using size_type = size_t;

    CustomAllocator() = default;

    template <class U>
    constexpr CustomAllocator(CustomAllocator<U> const&) noexcept {}

    template <typename T1>
    struct rebind {
        using other = CustomAllocator<T1>;
    };

    pointer allocate(size_type n = 0) {
        return (pointer)malloc(n * sizeof(value_type));
    }
    void deallocate(pointer p, size_type) {
        free(p);
    }
};
}  // namespace message_queue::testing
