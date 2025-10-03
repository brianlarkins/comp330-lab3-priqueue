#include "util.h"

/**
 * dprintf - prints debugging information (if toggled)
 * @str printf style format string
 * @return number of characters printed
 */
int dprint(const char *str, ...) {
  va_list ap;
  int ret;
  if (g.debug) {
    va_start(ap, str);
    ret = vfprintf(stderr, str, ap);
    va_end(ap);
    return ret;
  }
  return 0;
}
