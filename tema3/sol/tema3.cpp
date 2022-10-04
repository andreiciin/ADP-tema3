#include "mpi.h"
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <unistd.h>
#include <iostream>
#include <fstream>

#define SIZE 150000
#define CHUNK 2000

int main (int argc, char *argv[])
{
    int numtasks, rank;
    int topo[3][100] = { 0 }, cont_topo = -1, vec_topo1 = 0, vec_topo2;
    int coord = -1, hasDisplayed = 0;

    int N, v[SIZE] = { 0 }, total_topo, distrib, rest_topo, index = 0; 
    int out_v[SIZE] = { 0 }, out_k = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD,&rank);

    // std::vector<int> son;

    // coordonatorii citesc
    if (rank == 0) {
        std::ifstream file0("cluster0.txt");
        std::string text0;

        while (getline(file0, text0)) {

            int num = std::stoi(text0);
            if (cont_topo >= 0) topo[rank][cont_topo] = num;
            cont_topo++;
            // son.push_back(num);
        }

        file0.close();
    }

    if (rank == 1) {
        std::ifstream file1("cluster1.txt");
        std::string text1;

        while (getline(file1, text1)) {

            int num = std::stoi(text1);
            if (cont_topo >= 0) topo[rank][cont_topo] = num;
            cont_topo++;
        }

        file1.close();
    }

    if (rank == 2) {
        std::ifstream file2("cluster2.txt");
        std::string text2;

        while (getline(file2, text2)) {

            int num = std::stoi(text2);
            if (cont_topo >= 0) topo[rank][cont_topo] = num;
            cont_topo++;
        }

        file2.close();
    }

    // task 1
    if (rank < 3) { // coord comunica cu workerii lor si cu ceialti coordonatori
        
        // informez workerii cine e coord lor
        for (int i = 0; i < cont_topo; i++) {
            std::cout << "M(" << rank << "," << topo[rank][i] << ")\n";
            MPI_Send(&rank, 1, MPI_INT, topo[rank][i], 0, MPI_COMM_WORLD);
        }

        // trimit si primesc de la coordonatori topologia
        int recv_topo[3][100];
        MPI_Status status;
        for (int i = 0; i < 3; i++) {
            if (i != rank) {
                MPI_Send(&topo, 3 * 100, MPI_INT, i, 0, MPI_COMM_WORLD);
                 std::cout << "M(" << rank << "," << i << ")\n";

                MPI_Recv(&recv_topo, 3 * 100, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
                for(int j = 0 ; j < 100;j++){
                    topo[status.MPI_SOURCE][j] = recv_topo[status.MPI_SOURCE][j];
                }
            }
        } 

        // afisare finala coordonatori
        std::cout << rank << " -> ";
        for (int i = 0; i < 3; i++) {
            std::cout << i << ":";
            for (int j = 0; j < 4; j++) {
                if (topo[i][j] != 0) {
                    if (topo[i][j+1] != 0) {
                        std::cout << topo[i][j] <<",";
                    } else {
                        std::cout << topo[i][j] << " ";
                    }
                }
            }
        }
        std::cout << "\n";

    } else { // workerii comunica doar cu coordonatori
        
        // daca nu stiu coordonatorul, il primesc
        MPI_Status status;
        if (coord < 0) { 
            MPI_Recv(&coord, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        } 
    }

    // informez workerii despre topologia totala
    if (rank < 3) {
        // trimit topogia rezultata la workeri
        for (int i = 0; i < cont_topo; i++) {
            MPI_Send(&topo, 3 * 100, MPI_INT, topo[rank][i], 0, MPI_COMM_WORLD);
            std::cout << "M(" << rank << "," << topo[rank][i] << ")\n";
        }
    } else {
        // primesc topogia pentru fiecare worker
        int new_topo[3][100];
        MPI_Status status;
        MPI_Recv(&new_topo, 3 * 100, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        for(int i = 0 ; i<3;i++) {
            for(int j = 0 ; j<100;j++){
                topo[i][j] = new_topo[i][j];
            }
        }

        // afisare finala workeri
        std::cout << rank << " -> ";
        for (int i = 0; i < 3; i++) {
            std::cout << i << ":";
            for (int j = 0; j < 4; j++) {
                if (topo[i][j] != 0) {
                    if (topo[i][j+1] != 0) {
                        std::cout << topo[i][j] <<",";
                    } else {
                        std::cout << topo[i][j] << " ";
                    }
                }
            }
        }
        std::cout << "\n";
    }

    // task 2

    // aflu total workeri
    if (rank == 1) {
        MPI_Send(&cont_topo, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }
    if (rank == 2) {
        MPI_Send(&cont_topo, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }

    if (rank == 0) {
        MPI_Recv(&vec_topo1, 1, MPI_INT, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    if (rank == 0) {
        MPI_Recv(&vec_topo2, 1, MPI_INT, 2, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    if (rank == 0) {
        // initializare vector de catre procesul 0
        N = atoi(argv[1]);
        for (int i = 0; i < N; i++) v[i] = i;

        // numarul total de fii ai clusterelor 
        total_topo = cont_topo;
        total_topo += vec_topo1;
        total_topo += vec_topo2;
        // restul si numarul de elemente ale vectorului v per worker
        rest_topo = N % total_topo;
        distrib = N / total_topo; 
        
        // trimit numarul total de elemente catre restul coordonatorilor
        int v_send[3];
        v_send[0] = N; 
        v_send[1] = total_topo;  
        v_send[2] = cont_topo;
        MPI_Send(&v_send, 3, MPI_INT, 1, 0, MPI_COMM_WORLD);
        MPI_Send(&v_send, 3, MPI_INT, 2, 0, MPI_COMM_WORLD);
    }

    // primesc numarul total de elemente catre restul coordonatorilor
    if (rank == 1) {
        int v_recv[3];
        MPI_Recv(&v_recv, 3, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        N = v_recv[0];
        total_topo = v_recv[1];
        distrib = N / total_topo;
        index = v_recv[2] * distrib;

        int aux = index + distrib * cont_topo;
        MPI_Send(&aux, 1, MPI_INT, 2, 0, MPI_COMM_WORLD);
    }
    if (rank == 2) {
        int v_recv[3];
        MPI_Recv(&v_recv, 3, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        N = v_recv[0];
        total_topo = v_recv[1];
        distrib = N / total_topo;

        int aux;
        MPI_Recv(&aux, 1, MPI_INT, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        index = aux;   
    }

    // trimit de la 0 vectorul v si primesc v la 1 si 2
    if (rank == 0) {
        // trimit v
        MPI_Send(&v, SIZE, MPI_INT, 1, 0, MPI_COMM_WORLD);
        MPI_Send(&v, SIZE, MPI_INT, 2, 0, MPI_COMM_WORLD);
    } 
    // primesc v de la 0
    if (rank == 1) {
        MPI_Recv(&v, SIZE, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }
    if (rank == 2) {
        MPI_Recv(&v, SIZE, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    // distribuire si inmultire cu 2
    if (rank == 0) {
        // distribui vectorul workerilor pt coord 0
        for (int i = 0; i < cont_topo; i++) {

            int aux_vec[CHUNK] = { 0 };
            for (int j = 0; j < distrib; j++) {
                aux_vec[j] = v[i*distrib + j];
            }

            MPI_Send(&aux_vec, CHUNK, MPI_INT, topo[rank][i], 0, MPI_COMM_WORLD);
        }
    }
    if (rank == 1) {
        // distribui vectorul workerilor
        for (int i = 0; i < cont_topo; i++) {
           
            int aux_vec[CHUNK] = { 0 };
            for (int j = 0; j < distrib; j++) {
                aux_vec[j] = v[i*distrib + j + index];
            }

            MPI_Send(&aux_vec, CHUNK, MPI_INT, topo[rank][i], 0, MPI_COMM_WORLD);
        }
    }
    if (rank == 2) {
        // distribui vectorul workerilor
        rest_topo = N % total_topo;
        for (int i = 0; i < cont_topo; i++) {
           
            int aux_vec[CHUNK] = { 0 };
            if (i == cont_topo-1 && rest_topo > 0) {
                for (int j = 0; j < distrib + rest_topo; j++) {
                    aux_vec[j] = v[i*distrib + j + index];
                }
            } else {
                for (int j = 0; j < distrib; j++) {
                    aux_vec[j] = v[i*distrib + j + index];
                }
            }

            MPI_Send(&aux_vec, CHUNK, MPI_INT, topo[rank][i], 0, MPI_COMM_WORLD);
        }
    }
    
    // workerii primesc secventa de vector de la coordonatori 
    if (rank > 2) {

        if (coord == 0) {
            int vec[CHUNK];
            MPI_Recv(&vec, CHUNK, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int i = 0; i < CHUNK; i++) {
                if (vec[i] != 0) vec[i] *= 2;
                else if (i > 0 && vec[i] == 0) break;
            }

            // send double vec to coord
            MPI_Send(&vec, CHUNK, MPI_INT, 0, 0, MPI_COMM_WORLD);
        }

        if (coord == 1) {
            int vec[CHUNK];
            MPI_Recv(&vec, CHUNK, MPI_INT, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int i = 0; i < CHUNK; i++) 
                if (vec[i] != 0) vec[i] *= 2;
                else if (i > 0 && vec[i] == 0) break;

            // send double vec to coord
            MPI_Send(&vec, CHUNK, MPI_INT, 1, 0, MPI_COMM_WORLD);    
        }

        if (coord == 2) {
            int vec[CHUNK];
            MPI_Recv(&vec, CHUNK, MPI_INT, 2, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            for (int i = 0; i < CHUNK; i++) 
                if (vec[i] != 0) vec[i] *= 2;
                else if (i > 0 && vec[i] == 0) break;

            // send double vec to coord
            MPI_Send(&vec, CHUNK, MPI_INT, 2, 0, MPI_COMM_WORLD);        
        }

    }

    // fiecare coordonator va reuni vectorii de elemente dublate pe cluster
    if (rank == 0) {
        int k = 1;

        for (int i = 0; i < cont_topo; i++) {
            int v_recv[CHUNK];
            MPI_Recv(&v_recv, CHUNK, MPI_INT, topo[rank][i], 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
           
            for (int j = 0; j < CHUNK; j++) {
                
                if (v_recv[j] != 0) {
                    out_v[k] = v_recv[j];
                    k++;
                }
                if (j != 0 && v_recv[j] == 0) break; 
            }
        }
        out_k = k;
    }
    if (rank == 1) {
        int new_v[SIZE] = { 0 }, k = 0;

        for (int i = 0; i < cont_topo; i++) {
            int v_recv[CHUNK];
            MPI_Recv(&v_recv, CHUNK, MPI_INT, topo[rank][i], 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

           
            for (int j = 0; j < CHUNK; j++) {
                
                if (v_recv[j] != 0) {
                    new_v[k] = v_recv[j];
                    k++;
                }
                if (j != 0 && v_recv[j] == 0) break; 
            }
        }
        // trimitere pt afisare la coord 0
        MPI_Send(&new_v, SIZE, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }
    if (rank == 2) {
        int new_v[SIZE] = { 0 }, k = 0;

        for (int i = 0; i < cont_topo; i++) {
            int v_recv[CHUNK];
            MPI_Recv(&v_recv, CHUNK, MPI_INT, topo[rank][i], 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

           
            for (int j = 0; j < CHUNK; j++) {
                
                if (v_recv[j] != 0) {
                    new_v[k] = v_recv[j];
                    k++;
                }
                if (j != 0 && v_recv[j] == 0) break; 
            }
        }
        // trimitere catre coord 0
        MPI_Send(&new_v, SIZE, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }

    // reasamblare in coord 0 si afisare
    if (rank == 0) {

        int aux_v[SIZE];
        MPI_Recv(&aux_v, SIZE, MPI_INT, 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (int i = 0; i < SIZE; i++) {
            if (aux_v[i] != 0) {
                out_v[out_k] = aux_v[i];
                out_k++;
            } else break;
        }

        MPI_Recv(&aux_v, SIZE, MPI_INT, 2, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (int i = 0; i < SIZE; i++) {
            if (aux_v[i] != 0) {
                out_v[out_k] = aux_v[i];
                out_k++;
            } else break;
        }

        std::cout << "Rezultat: " << out_v[0] << " "; 
        for (int i = 1; i < SIZE; i++) 
            if (out_v[i] != 0) {
                std::cout << out_v[i] << " "; 
            }
        std::cout << "\n";
    }

    MPI_Finalize();

    return 0;
}