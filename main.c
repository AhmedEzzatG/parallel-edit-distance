#define _CRT_SECURE_NO_WARNINGS
#define HAVE_STRUCT_TIMESPEC

#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include "myqueue.h"
#include "edit_distance.h"

#pragma comment(lib, "msmpi.lib")
#pragma comment(lib, "pthreadVCE2.lib")//x86
//#pragma comment(lib,"pthreadVC2.lib")

#define WORD_LENGTH 10
#define N (1<<15)
#define Q (1<<4)
#define NUMBER_OF_ANSWERS 10
typedef char String[WORD_LENGTH];


typedef struct {
    int query_idx, string_idx;
} thread_request;

typedef struct {
    int cnt;
    String s;
} result_node;


int size_of_database, num_of_query;
String database[N], query[Q];
result_node output[Q][N];
int result_count[Q];
pthread_mutex_t result_lock[Q];


String local_database[N], local_query[Q];
result_node output2[Q][N];

pthread_t *thread_pool;

int compare_func(const void *a, const void *b) {
    result_node *l = (result_node *) a;
    result_node *r = (result_node *) b;
    int dif = l->cnt - r->cnt;
    if (dif != 0)
        return dif;
    return strcmp(l->s, r->s);
}

void *thread_work(void *);

int edit_distance(char *a, char *b);

void read_data(MPI_Comm row_comm, MPI_Comm col_comm);

int main(int argc, char **argv) {
    MPI_Init(&argc, &argv);
    int NUMBER_OF_GROUPS = (argc <= 1 ? 2 : atoi(argv[1]));
    int THREADS_COUNT = (argc <= 2 ? 10 : atoi(argv[2]));

    double start_time = MPI_Wtime();


    int world_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    MPI_Comm row_comm, col_comm;
    MPI_Comm_split(MPI_COMM_WORLD, world_rank % NUMBER_OF_GROUPS, world_rank, &row_comm);
    int row_rank, row_size;
    MPI_Comm_rank(row_comm, &row_rank);
    MPI_Comm_split(MPI_COMM_WORLD, row_rank, world_rank, &col_comm);
    int col_size;
    MPI_Comm_size(row_comm, &row_size);
    MPI_Comm_size(col_comm, &col_size);


    //read the database and queries for each node
    read_data(row_comm, col_comm);

    fprintf(stderr, "read data done in node %d\n", world_rank);


    for (int i = 0; i < Q; i++)
        pthread_mutex_init(&result_lock[i], NULL);

    thread_pool = malloc(sizeof(pthread_t) * THREADS_COUNT);
    for (int thread = 0; thread < THREADS_COUNT; thread++)
        pthread_create(&thread_pool[thread], NULL, thread_work, NULL);


    fprintf(stderr, "initialize thread pool done in node %d\n", world_rank);

    //send each pair we have as a request to one of the threads pool using the queue
    for (int i = 0; i < num_of_query; i++) {
        for (int j = 0; j < size_of_database; j++) {
            thread_request *p = malloc(sizeof(thread_request));
            p->query_idx = i;
            p->string_idx = j;
            queue_push(p);
        }
    }

    //wait until finish all the requests
    while (queue_size() > 0);

    fprintf(stderr, "calculations done in node %d\n", world_rank);

    for (int thread = 0; thread < THREADS_COUNT; thread++)
        pthread_cancel(thread_pool[thread]);
    for (int i = 0; i < Q; i++)
        pthread_mutex_destroy(&result_lock[i]);

    fprintf(stderr, "destroy threads done in node %d\n", world_rank);

    //for each query, gather the output from each row to the first node in row

    for (int i = 0; i < num_of_query; i++) {
        int cnt = (size_of_database < NUMBER_OF_ANSWERS ? size_of_database : NUMBER_OF_ANSWERS);

        MPI_Gather(&output[i], sizeof(result_node) * cnt, MPI_CHAR,
                   &output2[i], sizeof(result_node) * cnt, MPI_CHAR,
                   0, row_comm);
        if (row_rank == 0) {
            qsort(output2[i], cnt * row_size, sizeof(result_node),
                  compare_func);
        }
    }
    size_of_database *= row_size;

    //if this is the first node in row, gather the output in the node 0
    if (row_rank == 0) {
        MPI_Gather(output2, sizeof(output[0]) * num_of_query, MPI_CHAR,
                   output, sizeof(output[0]) * num_of_query, MPI_CHAR,
                   0, col_comm);
        num_of_query *= col_size;
    }

    //print the results
    if (world_rank == 0) {
        for (int i = 0; i < num_of_query; i++) {
            printf("query[%d] = \"%s\", top 10 =>\n", i, query[i]);
            for (int j = 0; j < size_of_database && j < NUMBER_OF_ANSWERS; j++)
                printf("%d. \"%s\", with edit distance = %d\n", j + 1, output[i][j].s, output[i][j].cnt);
            printf("================================================================\n");
        }
        double end_time = MPI_Wtime();
        printf("\n\nprocess done in %0.2f sec\n", end_time - start_time);
    }

    MPI_Comm_free(&row_comm);
    MPI_Comm_free(&col_comm);
    MPI_Finalize();
}


/**************************************** read data ****************************************/

char *random_string() {
    char *word = malloc(sizeof(char) * WORD_LENGTH);
    int len = WORD_LENGTH - 1;
    do {
        len = ((rand() * rand()) % WORD_LENGTH + WORD_LENGTH) % WORD_LENGTH;
    } while (len <= 2);
    for (int i = 0; i < len; i++) {
        word[i] = (char) (rand() % 26 + 'a');
    }
    word[len] = '\0';
    return word;
}


void read_data(MPI_Comm row_comm, MPI_Comm col_comm) {
    int world_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    int row_rank, row_size;
    MPI_Comm_rank(row_comm, &row_rank);
    MPI_Comm_size(row_comm, &row_size);
    int col_rank, col_size;
    MPI_Comm_rank(col_comm, &col_rank);
    MPI_Comm_size(col_comm, &col_size);
    size_of_database = num_of_query = 0;

    //generate random data
    if (world_rank == 0) {
        srand(0);
        size_of_database = N;
        num_of_query = Q;
        for (int i = 0; i < size_of_database; i++) {
            //generate random_string and copy it to database
            char *x = random_string();
            strcpy(database[i], x);
        }
        for (int i = 0; i < num_of_query; i++) {
            char *x = random_string();
            strcpy(query[i], x);
        }
        fprintf(stderr, "generate data is done\n");
    }

    //broadcast the database and scatter the query in the first column communicator
    if (row_rank == 0) {
        MPI_Bcast(&size_of_database, 1, MPI_INT, 0, col_comm);
        MPI_Bcast(database, size_of_database * sizeof(database[0]), MPI_CHAR, 0, col_comm);

        num_of_query /= col_size;
        MPI_Bcast(&num_of_query, 1, MPI_INT, 0, col_comm);
        MPI_Scatter(query, num_of_query * sizeof(query[0]), MPI_CHAR,
                    local_query, num_of_query * sizeof(query[0]), MPI_CHAR,
                    0, col_comm);
    }
    //scatter the database between each row, and broadcast the queries for each row
    size_of_database /= row_size;
    MPI_Bcast(&size_of_database, 1, MPI_INT, 0, row_comm);
    MPI_Scatter(database, size_of_database * WORD_LENGTH, MPI_CHAR,
                local_database, size_of_database * WORD_LENGTH, MPI_CHAR,
                0, row_comm);
    MPI_Bcast(&num_of_query, 1, MPI_INT, 0, row_comm);
    MPI_Bcast(local_query, num_of_query * WORD_LENGTH, MPI_CHAR, 0, row_comm);
}


/**************************************** thread work ****************************************/


void push_result(int query_idx, String s, int cost) {
    pthread_mutex_lock(&result_lock[query_idx]);

    int index = result_count[query_idx];
    output[query_idx][index].cnt = cost;
    strcpy(output[query_idx][index].s, s);

    if (++result_count[query_idx] > NUMBER_OF_ANSWERS) {
        qsort(output[query_idx], result_count[query_idx],
              sizeof(result_node), compare_func);
        result_count[query_idx] = NUMBER_OF_ANSWERS;
    }

    pthread_mutex_unlock(&result_lock[query_idx]);
}


void *thread_work(void *ignore) {
    while (1) {
        thread_request *val = (thread_request *) queue_pop();
        if (val == NULL)continue;
        int cost = edit_distance(local_database[val->string_idx], local_query[val->query_idx]);
        push_result(val->query_idx, local_database[val->string_idx], cost);
        free(val);
    }
    return NULL;
}