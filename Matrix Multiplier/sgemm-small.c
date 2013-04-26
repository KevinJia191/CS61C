#include <emmintrin.h> /* where intrinsics are defined */
//C[i+j*m] += A[i+k*m] * A[j+k*m];
void sgemm( int m, int n, float *A, float *C ) {
	int i, j, k, tmp2;
	float *tmp1;
	__m128 A2vector, A1vector;
	__m128 Cvector1 = _mm_setzero_ps();
	__m128 Cvector2 = _mm_setzero_ps();
	__m128 Cvector3 = _mm_setzero_ps();



for(j = 0; j < m; j++ )
{
	for(i = 0; i < m/12*12; i += 12)	//sets of 4 rows
	{
    		for(k = 0; k < n; ++k ) 	//column
    		{
			A1vector = _mm_set1_ps(*(A+j+k*m));		
			tmp1 = A + i + k*m;

			A2vector = _mm_loadu_ps( (__m128i*)(tmp1)); 				//load A[i+k*m] from 0-4	
			Cvector1 = _mm_add_ps(Cvector1, _mm_mul_ps( A2vector, A1vector));

			A2vector = _mm_loadu_ps( (__m128i*)(tmp1+4)); 			//load A[i+k*m] from 0-4	
			Cvector2 = _mm_add_ps(Cvector2, _mm_mul_ps( A2vector, A1vector));

			A2vector = _mm_loadu_ps( (__m128i*)(tmp1+8)); 			//load A[i+k*m] from 0-4	
			Cvector3 = _mm_add_ps(Cvector3, _mm_mul_ps( A2vector, A1vector));

    		}
		tmp1 = C + i + j*m;
		
		_mm_storeu_ps((tmp1), Cvector1);
		Cvector1 = _mm_setzero_ps();

		_mm_storeu_ps((tmp1 + 4), Cvector2);
		Cvector2 = _mm_setzero_ps();

		_mm_storeu_ps((tmp1 + 8), Cvector3);
		Cvector3 = _mm_setzero_ps();
	}

	//tail tail tail tail tail
	for(i = m/12*12; i<m; ++i)
	{
		for(k = 0; k<n/4*4; k=k+4)
		{
			tmp2 = i + j*m;
			C[tmp2] += A[i+k*m] * A[j+k*m];
			C[tmp2] += A[i+(k+1)*m] * A[j+(k+1)*m];
			C[tmp2] += A[i+(k+2)*m] * A[j+(k+2)*m];
			C[tmp2] += A[i+(k+3)*m] * A[j+(k+3)*m];
		}
		for(k = n/4*4; k<n; ++k)
		{
			C[i+j*m] += A[i+k*m] * A[j+k*m];
		}
	}
}


}

