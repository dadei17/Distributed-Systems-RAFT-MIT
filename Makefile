# This is the Makefile helping you submit the labs.
# and submit your lab with the following command:
#     $ make [lab1|lab2a|lab2b|lab2c|lab3a|lab3b|lab4a|lab4b]

LABS=" lab1 lab2a lab2b lab2c lab3a lab3b lab4a lab4b "

%: check-%
	@echo "Preparing $@-handin.tar.gz"
	@if echo $(LABS) | grep -q " $@ " ; then \
		echo "Tarring up your submission..." ; \
		tar cvzf $@-handin.tar.gz \
			"--exclude=src/main/pg-*.txt" \
			"--exclude=src/main/diskvd" \
			"--exclude=src/mapreduce/824-mrinput-*.txt" \
			"--exclude=src/main/mr-*" \
			"--exclude=mrtmp.*" \
			"--exclude=src/main/diff.out" \
			"--exclude=src/main/mrmaster" \
			"--exclude=src/main/mrsequential" \
			"--exclude=src/main/mrworker" \
			"--exclude=*.so" \
			Makefile src; \
		echo "Please submit the $@-handin.tar.gz via the Google Classroom web interface."; \
	else \
		echo "Bad target $@. Usage: make [$(LABS)]"; \
	fi

.PHONY: check-%
check-%:
	@echo "Checking that your submission builds correctly..."
	@./.check-build https://github.com/freeuni-distributed-systems/2021-labs.git $(patsubst check-%,%,$@)
