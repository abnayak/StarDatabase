#ifndef DEFS_H
#define DEFS_H


#define MAX_ANDS 20
#define MAX_ORS 20

#define PAGE_SIZE 131072

enum Target {
    Left, Right, Literal
};

enum CompOperator {
    LessThan, GreaterThan, Equals
};

enum Type {
    Int, Double, String
};

typedef enum {
    heap, sorted, btree
} fType;

typedef enum{
    writing, reading, ready
}Modes;

unsigned int Random_Generate();


#ifndef NumberToStr
#define NumberToStr( x ) dynamic_cast< std::ostringstream & >( \
        ( std::ostringstream() << std::dec << x ) ).str()
#endif

#endif

