#ifndef BUZZDB_QUERY_PARSER_H
#define BUZZDB_QUERY_PARSER_H

#include "QueryComponents.h"
#include <string>

QueryComponents parseQuery(const std::string& query);
void prettyPrint(const QueryComponents& components);

#endif // BUZZDB_QUERY_PARSER_H
