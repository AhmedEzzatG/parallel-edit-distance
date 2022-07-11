//
// Created by Ahmed Ezzat on 24/06/2022.
//

#ifndef CMAKE_BUILD_DEBUG_MYQUEUE_H
#define CMAKE_BUILD_DEBUG_MYQUEUE_H

#include <pthread.h>
#include <stdlib.h>


struct node {
    void* val;
    struct node* next;
};
typedef struct node node;
typedef struct node* nodeptr;


node* head = NULL, * tail = NULL;
pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;

void queue_push(void* val) {
    pthread_mutex_lock(&queue_lock);
    nodeptr cur = (nodeptr)malloc(sizeof(node));
    cur->val = val;
    cur->next = NULL;
    if (head == NULL) {
        head = cur;
        tail = head;
    }
    else {
        tail->next = cur;
        tail = cur;
    }
    pthread_mutex_unlock(&queue_lock);
}

void* queue_pop() {
    pthread_mutex_lock(&queue_lock);
    if (head == NULL) {
        pthread_mutex_unlock(&queue_lock);
        return NULL;
    }
    void* val = head->val;
    node* tmp = head;
    head = head->next;
    free(tmp);
    pthread_mutex_unlock(&queue_lock);
    return val;
}

int queue_size() {
    pthread_mutex_lock(&queue_lock);
    int cnt = 0;
    nodeptr cur = head;
    while (cur != NULL) {
        cnt++;
        cur = cur->next;
    }
    pthread_mutex_unlock(&queue_lock);
    return cnt;
}

#endif //CMAKE_BUILD_DEBUG_MYQUEUE_H

