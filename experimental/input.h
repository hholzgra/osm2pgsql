#ifndef INPUT_H
#define INPUT_H

int inputRead(void *context, char * buffer, int len);
int inputClose(void *context);
void *inputOpen(const char *name);
char inputGetChar(void *context);
int inputEof(void *context);

#endif
