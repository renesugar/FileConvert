#ifndef _STRINGUTIL_H
#define _STRINGUTIL_H

inline std::string rtrim(const std::string& s,
                         const std::string& delimiters = " \f\n\r\t\v" ) {
  size_t pos = s.find_last_not_of( delimiters );

  if (pos == std::string::npos)
    return std::string("");

  return s.substr( 0, pos + 1 );
}

inline std::string ltrim(const std::string& s,
                         const std::string& delimiters = " \f\n\r\t\v" ) {
  size_t pos = s.find_first_not_of( delimiters );

  if (pos == std::string::npos)
    return s;

  return s.substr( pos );
}

inline std::string trim(const std::string& s,
                        const std::string& delimiters = " \f\n\r\t\v" ) {
  return ltrim( rtrim( s, delimiters ), delimiters );
}

// http://www.cplusplus.com/faq/sequences/strings/split/

struct split {
  enum empties_t { empties_ok, no_empties };
};

template <typename Container>
Container& split(
                 Container&                            result,
                 const typename Container::value_type& s,
                 const typename Container::value_type& delimiters,
                 split::empties_t                      empties = split::empties_ok ) {
  result.clear();
  size_t current;
  size_t next = -1;
  do {
    if (empties == split::no_empties) {
      next = s.find_first_not_of(delimiters, next + 1);
      if (next == Container::value_type::npos)
        break;
      next -= 1;
    }
    current = next + 1;
    next = s.find_first_of(delimiters, current);
    result.push_back(s.substr(current, next - current));
  } while (next != Container::value_type::npos);
  return result;
}


#endif  // _STRINGUTIL_H
