//#include "VPumps.h"
#include "FileLog.h"
#include "../../smith/DiskCache.h"
#include "ShmPage.h"
extern "C" {
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <sys/mman.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
# include <sys/sendfile.h>
}

#ifdef ZQ_MS_WIN
  #define LOG_FOLDER "e:\\temp\\ShmSegmentTest.log"
#else
  #define LOG_FOLDER "/opt/TianShan/logs/ShmSegmentTest.log"
#endif
/*
class BufferAllocate : public ZQ::common::NativeThread
{
public:

	BufferAllocate(ZQ::memory::ShmSegments_LIPC& shmSegLIPC, int bufCount = 10): _shmSegLIPC(shmSegLIPC), _bufCount(bufCount)
	{}
	~BufferAllocate()
	{}
protected:
	virtual int run(void)
	{
		printf("enter BufferAllocate run()\n");
		std::vector<ZQ::memory::ShmPage::Ptr> shmSegs;
		int64 offsetOrigin = 0;
		//allocte buffer
		for(int i= 0; i < _bufCount; )
		{
			printf("try allocte buffer[%d:      %d]...\n", _bufCount, i+1);

			if(!_shmSegLIPC.isConnected())
			{
				SYS::sleep(1000);
				continue;
			}
			const std::string& uriOrigin = "TestSeg";
			ZQ::memory::ShmPage::Ptr pShmSeg = _shmSegLIPC.allocate(uriOrigin, offsetOrigin);

			if(pShmSeg != NULL)
			{
				printf("allocted buffer[%02d:1]=======> fd[%d] key[%s]\n", i +1 ,pShmSeg->fd(),  pShmSeg->key().c_str());
				shmSegs.push_back(pShmSeg);
				uint8 buf[26];
				memset(buf, 0, sizeof(buf));

				std::string strBuf = "";
				int len = 0;
				for(len = 0; len < sizeof(buf); len ++)
				{
					buf[len] = 'A' + len;

					char bufTemp[32];
					sprintf(bufTemp, "%c ", buf[len]);
					strBuf += std::string(bufTemp);
				}
                
				printf("write    buffer[%02d:1]=======> fd[%d] key[%s] buffer[%s], bufferSize[%d], startOffset[%d]\n\n",
					i +1 ,pShmSeg->fd(),  pShmSeg->key().c_str(), strBuf.c_str(), len, 0);

				pShmSeg->fill(buf, sizeof(buf), 0);
			}

			//�ڶ���allocete��ͬ��Buffer
			ZQ::memory::ShmPage::Ptr pShmSeg2 = _shmSegLIPC.allocate(uriOrigin, offsetOrigin);
			if(pShmSeg2 != NULL)
			{
				printf("allocted buffer[%02d:2]=======> fd[%d] key[%s]\n", i +1 ,pShmSeg2->fd(),  pShmSeg2->key().c_str());
				shmSegs.push_back(pShmSeg2);

				uint8 buf[26];
				memset(buf, 0, sizeof(buf));

				int nread = pShmSeg2->flush(buf, sizeof(buf), 5);

				std::string strBuf = "";
				for(int len = 0; len < nread; len ++)
				{
					char bufTemp[32];
					sprintf(bufTemp, "%c ", buf[len]);
					strBuf += std::string(bufTemp);
				}

				printf("read     buffer[%02d:2]=======> fd[%d] key[%s] buffer[%s], len[%d=====real read:%d], startOffset[%d]\n\n\n",
					i +1 ,pShmSeg2->fd(),  pShmSeg2->key().c_str(), strBuf.c_str(), sizeof(buf), nread, 0);
			}

			ZQ::memory::ShmPage::Ptr pShmSeg3 = _shmSegLIPC.allocate(uriOrigin, offsetOrigin  +1);
			if(pShmSeg3 != NULL)
			{
				printf("allocted buffer[%02d:3]=======> fd[%d] key[%s]\n", i +1 ,pShmSeg3->fd(),  pShmSeg3->key().c_str());
				shmSegs.push_back(pShmSeg3);

				pShmSeg3->fill(pShmSeg->fd(), 26, 10);
			}

			ZQ::memory::ShmPage::Ptr pShmSeg4 = _shmSegLIPC.allocate(uriOrigin, offsetOrigin +1);
			if(pShmSeg4 != NULL)
			{
				printf("allocted buffer[%02d:4]=======> fd[%d] key[%s]\n", i +1 ,pShmSeg4->fd(),  pShmSeg4->key().c_str());
				shmSegs.push_back(pShmSeg4);

				uint8 buf[26];
				memset(buf, 0, sizeof(buf));
				int nread = pShmSeg4->flush(buf, 26, 0);

				std::string strBuf = "";
				for(int len = 0; len < nread; len ++)
				{
					char bufTemp[32];
					sprintf(bufTemp, "%c ", buf[len]);
					strBuf += std::string(bufTemp);
				}

				printf("read     buffer[%02d:4]=======> fd[%d] key[%s] buffer[%s], len[%d=====real read:%d], startOffset[%d]\n\n\n",
					i +1 ,pShmSeg4->fd(),  pShmSeg4->key().c_str(), strBuf.c_str(), sizeof(buf), nread, 0);
			}

			offsetOrigin += SEGMENT_SIZE;
			i++;
			SYS::sleep(1000);
		}

		///free buffer
		std::vector<ZQ::memory::ShmPage::Ptr>::iterator iorShmSeg = shmSegs.begin();
		int i = 0;
		while(iorShmSeg != shmSegs.end())
		{
			ZQ::memory::ShmPage::Ptr& pShmSeg = *iorShmSeg;
			printf("free buffer[%d]=======> fd[%d] key[%s]\n", i +1 ,pShmSeg->fd(),  pShmSeg->key().c_str());

			_shmSegLIPC.free(pShmSeg);
			
			pShmSeg = NULL;

			iorShmSeg++;
			i++;
		}

		printf("leave BufferAllocate run()\n");
		return 1;
	}
private:
	ZQ::memory::ShmSegments_LIPC& _shmSegLIPC;
	int _bufCount;
};
void test()
{

#define OPEN_FLAG (O_RDWR|O_CREAT)
#define OPEN_MODE 00777
	int fd = shm_open("Test-0001",  OPEN_FLAG, OPEN_MODE);
	if(fd <0)
		return;

	if(ftruncate(fd, SEGMENT_SIZE) == -1)
		return;
    // seek position 0
	lseek(fd, 0, SEEK_SET);

	//write data from 0
	unsigned char buf[27] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

	{
		uint8 * ptr = (uint8 *)::mmap(NULL, SEGMENT_SIZE, PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0);

		if (ptr == MAP_FAILED)
		{
			printf("error %d \n", errno);
			return;
		}

		memcpy(ptr, buf, 26);

		int ret = munmap(ptr, SEGMENT_SIZE);
		if(-1 == ret)
		  {
			printf("error %d \n", errno);
		        return;
		  }
	}

	// seek position 10
	lseek(fd, 10, SEEK_SET);

	//read data
	{
		uint8 * ptr = (uint8 *)::mmap(NULL, SEGMENT_SIZE, PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0);

		if (ptr == MAP_FAILED)
		{
			printf("error %d \n", errno);
			ptr = NULL;
			return;
		}

		unsigned char buf[27];

		memcpy(buf, ptr, 10);

		std::string strBuf;
		for(int len = 0; len < 10; len ++)
		{
			char bufTemp[32];
			sprintf(bufTemp, "%c ", buf[len]);
			strBuf += std::string(bufTemp);
		}
		printf("%s\n", strBuf.c_str());

		int ret = munmap(ptr, SEGMENT_SIZE);
		if(-1 == ret)
		{
		   printf("error %d \n", errno);
		   return;
		}
	}
	int ret = shm_unlink("Test-0001");
	if(-1 == ret)
	{
		printf("error %d \n", errno);
		return;
	}
}

void testShmSegmentLocal(ZQ::eloop::Loop& loop)
{
	std::string logFolder = "/opt/TianShan/logs/ShmSegmentTest_Service.log";

	ZQ::common::FileLog flog(logFolder.c_str(), ZQ::common::Log::L_DEBUG);
	ZQ::memory::ShmPages_Local shmServer(flog, loop, 100);
	std::vector<ZQ::memory::ShmPage::Ptr> shmSegs;
	int64 offsetOrigin = 0;
	int _bufCount = 5;
	//allocte buffer
	for(int i= 0; i < _bufCount; ++i)
	{
		printf("try allocte buffer[%d:      %d]...\n", _bufCount, i+1);

		const std::string& uriOrigin = "TestShmSegment";
		ZQ::memory::ShmPage::Ptr pShmSeg = shmServer.allocate(uriOrigin, offsetOrigin);

		if(pShmSeg != NULL)
		{
			printf("allocted buffer[%02d:1]=======> fd[%d] key[%s]\n", i +1 ,pShmSeg->fd(),  pShmSeg->key().c_str());
			shmSegs.push_back(pShmSeg);
			uint8 buf[26];
			memset(buf, 0, sizeof(buf));

			std::string strBuf = "";
			int len = 0;
			for(len = 0; len < sizeof(buf); len ++)
			{
				buf[len] = 'A' + len;

				char bufTemp[4];
				sprintf(bufTemp, "%c ", buf[len]);
				strBuf += std::string(bufTemp);
			}

			//printf("write    buffer[%02d:1]=======> fd[%d] key[%s] buffer[%s], bufferSize[%d], startOffset[%d]\n\n",
			//	i +1 ,pShmSeg->fd(),  pShmSeg->key().c_str(), strBuf.c_str(), len, 0);

			pShmSeg->fill(buf, sizeof(buf), 0);

			pShmSeg->fill(buf, sizeof(buf), 26);

			pShmSeg->fill(buf, 10, 10);

			printf("write    buffer[%02d:1]=======> fd[%d] key[%s] length[%d]\n\n", i +1 ,pShmSeg->fd(),  pShmSeg->key().c_str(), pShmSeg->getLength());
		}

		//�ڶ���allocete��ͬ��Buffer
		ZQ::memory::ShmPage::Ptr pShmSeg2 = shmServer.allocate(uriOrigin, offsetOrigin);
		if(pShmSeg2 != NULL)
		{
			printf("allocted buffer[%02d:2]=======> fd[%d] key[%s]\n", i +1 ,pShmSeg2->fd(),  pShmSeg2->key().c_str());
			shmSegs.push_back(pShmSeg2);

			uint8 buf[100];
			memset(buf, 0, sizeof(buf));

			int startOffset= 0;
			int nread = pShmSeg2->flush(buf, sizeof(buf), startOffset);

			std::string strBuf = "";
			for(int len = 0; len < nread; len ++)
			{
				char bufTemp[4];
				sprintf(bufTemp, "%c ", buf[len]);
				strBuf += std::string(bufTemp);
			}

			printf("read     buffer[%02d:2]=======> fd[%d] key[%s] buffer[%s], len[%d=====real read:%d], startOffset[%d]\n\n\n",
				i +1 ,pShmSeg2->fd(),  pShmSeg2->key().c_str(), strBuf.c_str(), sizeof(buf), nread, startOffset);
		}

		offsetOrigin+= 2*1024*1024;
	}
}

void testShmSegmentLocal2(ZQ::eloop::Loop& loop)
{
	std::string logFolder = "/opt/TianShan/logs/ShmSegmentTest_Service.log";

	ZQ::common::FileLog flog(logFolder.c_str(), ZQ::common::Log::L_DEBUG);
	ZQ::memory::ShmPages_Local shmServer(flog, loop, 20);
	std::vector<ZQ::memory::ShmPage::Ptr> shmSegs;
	int64 offsetOrigin = 0;
	int _bufCount = 25;
	const std::string& uriOrigin = "TestShmSegment";
	int count = 2;
	while (count > 0)
	{
		for(int i =0; i < _bufCount; ++i)
		{
			printf("try allocte buffer[%d====>%02d]...\n", _bufCount, i+1);
			ZQ::memory::ShmPage::Ptr pShmSeg = shmServer.allocate(uriOrigin, offsetOrigin);
			if(pShmSeg ==  NULL)
				printf("allocted buffer[%d====>%02d] failed\n", i +1  );
			else
			{
				printf("allocted buffer[%d====>%02d] fd[%d] key[%s]\n", _bufCount, i + 1 ,pShmSeg->fd(),  pShmSeg->key().c_str());
				shmSegs.push_back(pShmSeg);
			}
			offsetOrigin+= 1*1024*1024;
		}

		for(int i = 5; i < 10;  ++i)
		{
			uint8* buf = new uint8[SEGMENT_SIZE];
			memset(buf, 'a', SEGMENT_SIZE);

			int nwrite = shmSegs[i]->fill(buf, SEGMENT_SIZE, 0);

			printf("filled buffer[%d====>%02d]=======> fd[%d] key[%s]\n", _bufCount, i +1, shmSegs[i]->fd(),  shmSegs[i]->key().c_str());

		}

		offsetOrigin = 5* 1024*1024;

		for(int i = 5; i < 10; ++i)
		{
			printf("try allocte buffer[%d====>%02d]...\n", _bufCount, i+1);
			ZQ::memory::ShmPage::Ptr pShmSeg = shmServer.allocate(uriOrigin, offsetOrigin);
			if(pShmSeg ==  NULL)
				printf("re allocted buffer[%d====>%02d] failed\n", i +1  );
			else
			{
				printf("re allocted buffer[%d====>%02d] fd[%d] key[%s]\n", _bufCount, i + 1 ,pShmSeg->fd(),  pShmSeg->key().c_str());
			}
			offsetOrigin+= 1*1024*1024;
		}

		for(int i = 0; i < shmSegs.size();  ++i)
		{
			printf("free buffer[%d====>%02d]=======> fd[%d] key[%s]\n", _bufCount, i +1, shmSegs[i]->fd(),  shmSegs[i]->key().c_str());
			shmServer.free(shmSegs[i]);
		}

		shmSegs.clear();

		--count;

		offsetOrigin= 1;
	}
}
*/

class BufferAllocate : public ZQ::common::NativeThread
{
public:

	BufferAllocate(ZQ::memory::ShmPages_Local& shmServer): _shmServer(shmServer)
	{}
	~BufferAllocate()
	{}
protected:
	virtual int run(void)
	{
		//testShmSegmentLocal3();
		//testReadWrite();
		testDiskCacheRead();
		printf("leave BufferAllocate run()\n");
		return 1;
	}

	void testShmSegmentLocal3();
	void testReadWrite();
	void testDiskCacheRead();
private:
	ZQ::memory::ShmPages_Local& _shmServer;
};
void BufferAllocate::testShmSegmentLocal3()
{
	ZQ::memory::ShmPages_Local& shmServer = _shmServer;
	std::vector<ZQ::memory::LargeContentPage::Ptr> shmSegs;
	int64 offsetOrigin = 0;
	int _bufCount = 10;
	const std::string& uriOrigin = "TestShmSegment";
//	int count = 10;

	for(int i =0; i < _bufCount; )
	{
		//printf("try allocte buffer[%d====>%02d]...\n", _bufCount, i+1);
		ZQ::memory::LargeContentPage::Ptr pShmSeg = shmServer.allocate(uriOrigin, offsetOrigin);
		if(pShmSeg ==  NULL)
		{
			printf("allocted buffer[%d] failed\n", i +1  );
			SYS::sleep(500);
			continue;
		}
		else
		{
			printf("allocted buffer[%d====>%02d] fd[%d] key[%s] length[%d] maxLength[%d] is [%s]\n", 
				_bufCount, i + 1 ,pShmSeg->fd(),  pShmSeg->key().c_str(), pShmSeg->getLength(),pShmSeg->getCapacity(),
				pShmSeg->getLength() == pShmSeg->getCapacity() ? "full buffer" : "new allocte");

			shmSegs.push_back(pShmSeg);

			if(pShmSeg->getLength() == pShmSeg->getCapacity())
			{
				uint8* buf = new uint8[shmSegs[i]->getLength()];

				printf("read buffer[%02d:2]=======> fd[%d] key[%s]\n",i +1 ,shmSegs[i]->fd(),  shmSegs[i]->key().c_str());
				if(buf != NULL)
				{
					int readLen = shmSegs[i]->flush(buf, shmSegs[i]->getLength(), 0);
					printf("	read from disk cache front 128 byte: ");
					for(int len = 0; len < 128; len ++)
					{
						printf("%c", buf[len]);
					}
					printf("\n	read from disk cache end   128 byte: ");
					for(int len = shmSegs[i]->getLength() - 128; len < shmSegs[i]->getLength(); len ++)
					{
						printf("%c", buf[len]);
					}
					printf("\n");

					delete buf;
				}
			
			}
		}

		offsetOrigin+= 2*1024*1024;
		++i;
	}

	SYS::sleep(10 * 1000 * 1);

	for(int i = 2; i < 5 && shmSegs.size() > 5;  ++i)
	{
		uint8* buf = new uint8[SEGMENT_SIZE];
		memset(buf, 'a', SEGMENT_SIZE);

		int nwrite = shmSegs[i]->fill(buf, SEGMENT_SIZE, 0);

		printf("filled buffer[%d====>%02d]=======> fd[%d] key[%s]\n", _bufCount, i +1, shmSegs[i]->fd(),  shmSegs[i]->key().c_str());
		delete  []buf;
		if(nwrite == SEGMENT_SIZE)
		{
			printf("full buffer notify: fd[%d] key[%s]\n",shmSegs[i]->fd(),  shmSegs[i]->key().c_str());
			shmServer.onFull(shmSegs[i]);
		}
	}

	offsetOrigin = 0;
	for(int i = 0; i < 5 && shmSegs.size() > 5; ++i)
	{
		//printf("try allocte buffer[%d====>%02d]...\n", _bufCount, i+1);
		ZQ::memory::LargeContentPage::Ptr pShmSeg = shmServer.allocate(uriOrigin, offsetOrigin);
		if(pShmSeg ==  NULL)
			printf("re allocted buffer[%d] failed\n", i +1  );
		else
		{
			printf("re allocted buffer[%d====>%02d] fd[%d] key[%s]\n", _bufCount, i + 1 ,pShmSeg->fd(),  pShmSeg->key().c_str());
			shmSegs.push_back(pShmSeg);
		}
		offsetOrigin+= 2*1024*1024;
	}

	SYS::sleep(1000 * 10);

	offsetOrigin = 0;
	for(int i = 0; i < 5 && shmSegs.size() > 5; ++i)
	{
		//printf("try allocte buffer[%d====>%02d]...\n", _bufCount, i+1);
		ZQ::memory::LargeContentPage::Ptr pShmSeg = shmServer.allocate(uriOrigin, offsetOrigin);
		if(pShmSeg ==  NULL)
			printf("2-rd re allocted buffer[%d] failed\n", i +1  );
		else
		{
			printf("2-rd re allocted buffer[%d====>%02d] fd[%d] key[%s]\n", _bufCount, i + 1 ,pShmSeg->fd(),  pShmSeg->key().c_str());
			shmSegs.push_back(pShmSeg);
		}
		offsetOrigin+= 2*1024*1024;
	}

	for(int i = 0; i < shmSegs.size(); i++)
	{
		printf("free buffer[%d====>%02d]\n", i, shmSegs[i]->fd());
		shmSegs[i] = NULL;
	}
	shmSegs.clear();
	shmServer.close();
	printf("exit testShmSegmentLocal3()\n");
}

void BufferAllocate::testReadWrite()
{
	ZQ::memory::ShmPages_Local& shmServer = _shmServer;
	std::vector<ZQ::memory::LargeContentPage::Ptr> shmSegs;
	int64 offsetOrigin = 0;
	int _bufCount = 10;
	const std::string& uriOrigin = "TestShmSegmentRW";

	printf("allocate buffer......\n");
	for(int i =0; i < _bufCount; )
	{
		ZQ::memory::LargeContentPage::Ptr pShmSeg = shmServer.allocate(uriOrigin, offsetOrigin);
		if(pShmSeg ==  NULL)
		{
			printf("allocted buffer[%d] failed\n", i +1  );
			SYS::sleep(500);
			continue;
		}
		else
		{
			printf("allocted buffer[%d====>%02d] fd[%d] key[%s] length[%d] maxLength[%d] is [%s]\n", 
				_bufCount, i + 1 ,pShmSeg->fd(),  pShmSeg->key().c_str(), pShmSeg->getLength(),pShmSeg->getCapacity(),
				pShmSeg->getLength() == pShmSeg->getCapacity() ? "full buffer" : "new allocte");
			shmSegs.push_back(pShmSeg);

			if(pShmSeg->getLength() == pShmSeg->getCapacity())
			{
				uint8* buf = new uint8[shmSegs[i]->getLength()];

				printf("read buffer[%02d:2]=======> fd[%d] key[%s]\n",i +1 ,shmSegs[i]->fd(),  shmSegs[i]->key().c_str());
				if(buf != NULL)
				{
					int readLen = shmSegs[i]->flush(buf, shmSegs[i]->getLength(), 0);

				/*	for(int len = 0; len < shmSegs[i]->getLength(); len ++)s
					{
						printf("%c", buf[len]);
					}
					//printf(".......%c", buf[shmSegs[i]->getLength() -2]);
					//printf("%c",buf[ shmSegs[i]->getLength() -1]);

					printf("], len[%d=====real read:%d]\n\n", shmSegs[i]->getLength(), readLen );
			    */

					printf("	read from disk cache front 128 byte: ");
					for(int len = 0; len < 128; len ++)
					{
						printf("%c", buf[len]);
					}
					printf("\n	read from disk cache end   128 byte: ");
					for(int len = shmSegs[i]->getLength() - 128; len < shmSegs[i]->getLength(); len ++)
					{
						printf("%c", buf[len]);
					}
					printf("\n");

					delete buf;
				}
			}
		}

		offsetOrigin+= 2*1024*1024;
		++i;
	}

	SYS::sleep( 1* 1000 * 1);

	printf("fill buffer......\n");
	for(int i = 0; i < shmSegs.size();  ++i)
	{
		uint8* buf = new uint8[SEGMENT_SIZE ];
		memset(buf, '2', SEGMENT_SIZE);
		buf[0] = 'a';

		char  ch = 'a';
		for(int j = 0; j < 26;  j++)
		{
			buf[SEGMENT_SIZE - 26 + j] = ch ++;
		}

		int nwrite = shmSegs[i]->fill(buf, SEGMENT_SIZE -5, 0);

		for(int j = 0; j < 5;  j++)
		{
			buf[SEGMENT_SIZE - 5 + j] = 67;
		}
		int nwrite2 = shmSegs[i]->fill(buf + SEGMENT_SIZE - 5, 5 , -1);

		delete  []buf;

		printf("filled buffer[%d====>%02d]=======> fd[%d] key[%s] len[%d/%d/%d/%d]\n", _bufCount, i +1, shmSegs[i]->fd(),  shmSegs[i]->key().c_str(), 
			nwrite,nwrite2, shmSegs[i]->getLength(), shmSegs[i]->getCapacity());
		if(shmSegs[i]->getLength() == shmSegs[i]->getCapacity())
		{
			printf("full buffer notify: fd[%d] key[%s]\n",shmSegs[i]->fd(),  shmSegs[i]->key().c_str());
			shmServer.onFull(shmSegs[i]);
		}
	}
/*
	int readLength = 128;
	for(int i = 0; i < shmSegs.size();  ++i)
	{
		uint8* buf = new uint8[shmSegs[i]->getCapacity()];
		memset(buf , 0, shmSegs[i]->getCapacity());

		printf("\nread buffer, key[%s]\n", shmSegs[i]->key().c_str());

		if(buf != NULL)
		{
			int readLen = shmSegs[i]->flush(buf, readLength, 0);
			printf("	read from buffer front data %d byte:", readLen);
			for(int len = 0; len < readLen; len ++)
			{
				printf("%c", buf[len]);
			}

			printf("\n");
			memset(buf , 0, shmSegs[i]->getCapacity());
			readLen = shmSegs[i]->flush(buf, readLength, shmSegs[i]->getLength() - readLength);
			printf("\n	read from buffer end data %d byte:", readLen);
			for(int len = 0; len < readLen; len ++)
			{
				printf("%c", buf[len]);
			}

			//printf("\nfull buffer[%02d:2]=======> fd[%d] key[%s] buffer[%s], len[%d=====real read:%d]\n\n\n",
			//	i +1 ,shmSegs[i]->fd(),  shmSegs[i]->key().c_str(), strBuf.c_str(), sizeof(buf), readLen );

			delete []buf;

			printf("\n");
		}
	}

	SYS::sleep(30 * 1000);
*/
	printf("\nfree buffer......\n");

	for(int i = 0; i < shmSegs.size(); i++)
	{
		printf("free buffer[%d====>%02d]\n", i, shmSegs[i]->fd());
		shmSegs[i] = NULL;
	}
    
	SYS::sleep(30 * 1000);
	shmSegs.clear();
	shmServer.close();
	printf("exit testReadWrite()\n");
}

void BufferAllocate::testDiskCacheRead()
{
	ZQ::memory::ShmPages_Local& shmServer = _shmServer;
	std::vector<ZQ::memory::LargeContentPage::Ptr> shmSegs;
	int64 offsetOrigin = 0;
	int _bufCount = 10;
	const std::string& uriOrigin = "ngodc2://10.8.8.171:10080/testB020170510001001xor.com.0X0000";

	printf("allocate buffer......\n");
	for(int i =0; i < _bufCount; )
	{
		ZQ::memory::LargeContentPage::Ptr pShmSeg = shmServer.allocate(uriOrigin, offsetOrigin);
		if(pShmSeg ==  NULL)
		{
			printf("allocted buffer[%d] failed\n", i +1  );
			SYS::sleep(500);
			continue;
		}
		else
		{
			printf("allocted buffer[%d====>%02d] fd[%d] key[%s] length[%d] maxLength[%d] is [%s]\n", 
				_bufCount, i + 1 ,pShmSeg->fd(),  pShmSeg->key().c_str(), pShmSeg->getLength(),pShmSeg->getCapacity(),
				pShmSeg->getLength() == pShmSeg->getCapacity() ? "full buffer" : "new allocte");

			shmSegs.push_back(pShmSeg);

			std::string filename = "/opt/TianShan/temp/" + pShmSeg->key();
			int npos = pShmSeg->key().find_last_of('/');
			if(npos > 0)
				filename = "/opt/TianShan/temp/" + pShmSeg->key().substr(npos +1);

			int fd = open(filename.c_str(), O_RDWR | O_CREAT); // | O_TRUNC);
			if (fd  > 0)
			{
				lseek(pShmSeg->fd(), 0, SEEK_SET);

				ssize_t  nbytes = ::sendfile(fd, pShmSeg->fd(), NULL, (pShmSeg->getLength() + 512));
             // int nbytes = write(fd, pShmSeg->ptr(), pShmSeg->getLength());
				printf("write filename[%s] ret[%d]\n", filename.c_str(), nbytes);

			//	lseek(pShmSeg->fd, 0, SEEK_SET);
				::close(fd);
			}

		}

		offsetOrigin+= 2*1024*1024;
		++i;
	}

	printf("\nfree buffer......\n");

	for(int i = 0; i < shmSegs.size(); i++)
	{
		printf("free buffer[%d====>%02d]\n", i, shmSegs[i]->fd());
		shmSegs[i] = NULL;
	}
    
	SYS::sleep(30 * 1000);
	shmSegs.clear();
	shmServer.close();
	printf("exit testDiskCacheRead()\n");
}
void test()
{

	std::string segname= "TESTSHM_1";

#define OPEN_FLAG (O_RDWR|O_CREAT)
#define OPEN_MODE 00777

	int fd = shm_open(segname.c_str(),  OPEN_FLAG, OPEN_MODE);
	if (fd <0)
	{
		printf("shm_open failed fd[%d]:%s\n", fd, segname.c_str());
		return ;
	}

	printf("shm_open open fd[%d]:%s\n", fd, segname.c_str());

	int nFtruncate = ftruncate(fd, HEADLEN_SegmentFile + SEGMENT_SIZE);

	if (nFtruncate == -1)
	{
		printf("ftruncate failed fd[%d]:%s", fd, segname.c_str());
		return ;
	}

	printf("ftruncate fd[%d][%d]:%s\n", fd, HEADLEN_SegmentFile + SEGMENT_SIZE, segname.c_str());


	char *Tmp = (char*)malloc(HEADLEN_SegmentFile + SEGMENT_SIZE);
	memset(Tmp,0,HEADLEN_SegmentFile + SEGMENT_SIZE);
	int byteWrite = write(fd,Tmp,HEADLEN_SegmentFile + SEGMENT_SIZE);
	::free(Tmp);


	printf("write fd[%d]len[%d]:%s\n", fd, byteWrite, segname.c_str());

}
int  main(int argc, char** argv)
{
//	test();
	///DiskCache Service Test
	if(argc >= 2)
	{
		printf("DiskCache Service test:\n");
		std::string logFolder = "/opt/TianShan/logs/DiskCacheServiceTest.log";

		ZQ::common::FileLog flog(logFolder.c_str(), ZQ::common::Log::L_DEBUG);
		ZQ::eloop::Loop loop(false);
		std::string diskCachePipeName =  "/var/run/DiskCachePipe";
		if(access(diskCachePipeName.c_str(),F_OK) == 0)
		{
			SYS::sleep(1000);
			bool bret = remove(diskCachePipeName.c_str());
			printf("delete file [%s]  %s \n", diskCachePipeName.c_str(), bret == 0?"success":"failure");
			if(bret)
			{
				return -1;
			}
		}
		ZQ::Stream::DiskCacheApp diskCacheService(flog, loop, diskCachePipeName.c_str());
		ZQ::Stream::CacheDir::Ptr cachePtr = diskCacheService.addCacheDir("/home/li.huang/Cache", 2*1024);
		loop.run(ZQ::eloop::Loop::Default);	
	}

	///ShmPage Test
	{
		printf("ShmPage test:\n");
		ZQ::eloop::Loop loop(false);
		std::string logFolder = "/opt/TianShan/logs/ShmSegmentTest_Service.log";
		ZQ::common::FileLog flog(logFolder.c_str(), ZQ::common::Log::L_DEBUG);
		ZQ::memory::ShmPages_Local shmSegment_local(flog, loop, 20, 5000, 5 * 60 * 1000, 60000);
		shmSegment_local.setVerbosity(1543);

		BufferAllocate bufferAllocte(shmSegment_local);
		bufferAllocte.start();

		loop.run(ZQ::eloop::Loop::Default);
	}

/*	std::string logFolder = "/opt/TianShan/logs/ShmSegmentTest_Client.log";

	bool bServer = false;
	if(argc >= 2)
	{
		printf("LIPC Server Mode:\n");
		logFolder = "/opt/TianShan/logs/ShmSegmentTest_Service.log";
		bServer = true;
	}

	ZQ::common::FileLog flog(logFolder.c_str(), ZQ::common::Log::L_DEBUG);
	ZQ::eloop::Loop loop(false);

	if(bServer)
	{
	    std::string serverPipeName="/var/run/ShmSegments";
		if(access(serverPipeName.c_str(),F_OK) == 0)
		{
			SYS::sleep(1000);
			bool bret = remove(serverPipeName.c_str());
			printf("delete file [%s]  %s \n", serverPipeName.c_str(), bret == 0?"success":"failure");
			if(bret)
			{
				return -1;
			}
		}
		ZQ::memory::ShmPages_Local shmServer(flog, loop, 50 ,100);
		loop.run(ZQ::eloop::Loop::Default);	
	}
	else
	{
		ZQ::memory::ShmSegments_LIPC shmClient(flog, loop, 60000);
		BufferAllocate bufferAllocte(shmClient, 10);
		bufferAllocte.start();
	    loop.run(ZQ::eloop::Loop::Default);
	}
*/
	printf("exit\n");
	return 0;
}
