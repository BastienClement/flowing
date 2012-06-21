###
  Copyright (C) 2012 Copperflake Software

  Permission is hereby granted, free of charge, to any person obtaining
  a copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so, subject to
  the following conditions:

  The above copyright notice and this permission notice shall be included
  in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
  CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
  TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
###

# Check variable type with support for Arrays [private]
flowing_typeof = (v) -> if Array.isArray(v) then "array" else typeof v

# Delay execution of a function
delay = (fn) -> process.nextTick fn

# Handle a flow execution
class FlowingContext
	constructor: (@steps, @params, @cb) ->
		@step = -1
		@data = {}
		@done = false
	
	# Jump to a label
	jump: (label) ->
		for step, i in @steps
			if step._label == label
				idx = i
				break
		
		if not idx?
			@exit_fail new Error "Jump to an undefined label '#{label}'"
			return false
		
		@step = idx-1
		return true
	
	# Exit handlers
	exit_success: (args) ->
		return if @done
		if typeof @cb == "function"
			args.unshift undefined # Add an undefined error before arguments
			@cb.apply null, args
		@done = true
		return
		
	exit_fail: (e) ->
		return if @done
		if typeof @cb == "function"
			@cb e
		@done = true
		return
		
	exit: (e, args) ->
		if e then @exit_fail e else @exit_success args
	
	# Main controller function
	next: (e, args) ->
		return if @done
		
		try
			# Select next step
			while step = @steps[++@step]
				# Error tag must match error state and filter must be matched if defined
				break if (!!e == !!step._error) && (not step._filter? || step._filter e)
			
			if step
				# Next step is defined
				if step._flow
					# This step is a sub-flow
					step args..., (e, args...) =>
						@next e, args
				else if step._delay 
					# This step must be delayed
					delay => @run step, e, args
				else
					# Simple execution
					@run step, e, args
			else
				# Next step isnt defined, return to caller
				@exit e, args
		catch e
			# Something goes wrong when executing the step
			@exit_fail e
		
		return
	
	# Run a given step
	run: (step, e, args) ->
		# Delegate exposed to the step
		delegate = new StepDelegate @
		
		# Execute the step
		args = [e] if e
		try
			result = step.apply delegate, args
		catch e
			delegate.error e
		
		# Unlock & flush parallels if synchronous
		delegate._parallel_unlock()
		
		# If step is done or async, nothing else is required
		return if delegate._done || delegate._async || step._async
		
		# Auto trigger next step
		delegate.done result

# Interface to a step execution
class StepDelegate
	constructor: (@ctx) ->
		@_done = false
		@_async = false
		
		# Expose data
		@data = @ctx.data
	
		# Parallel stuff
		@_p_count = 0
		@_p_done  = 0
		@_p_idx   = 0
		@_p_args  = []
		@_p_lock  = true
	
	# Flow storage
	set: (name, value) ->
		if typeof name == "object"
			# Keys / Values map
			@ctx.data[key] = val for key, val of name
		else
			@ctx.data[name] = value
		
		return
	
	get: (name) -> @ctx.data[name]
	
	# Node-compatible callback for next step
	next: ->
		@_async = true
		return (e, args...) =>
			if @_done then return else @_done = true
			@ctx.next e, args
			return
	
	# Jump to an explicit label
	goto: (label, args...) ->
		if @_done then return else @_done = true
		if @ctx.jump label
			@ctx.next undefined, args
		return
	
	# Delayed jump to an explicit label
	jump: (label, args...) ->
		return if @_done
		return @ctx.jump label
	
	# Terminate the current step with success
	done: (args...) ->
		if @_done then return else @_done = true
		@ctx.next undefined, args
		return
	
	# Terminate the current step on an error
	error: (e) ->
		if @_done then return else @_done = true
		@ctx.next e, []
		return
	
	# Exit the current flow with success
	exit: (args...) ->
		if @_done then return else @_done = true
		@ctx.exit_success args
		return
	
	# Exit the current flow with an error
	fail: (e) ->
		if @_done then return else @_done = true
		@ctx.exit_fail e
		return
	
	# Give partial results to the next step
	partial: (args...) ->
		return if @_done
		@_async = true
		
		@_p_count++
		@_p_done++
		@_p_args[@_p_idx++] = arg for arg in args
		
		return
	
	# Execute functions in parallel
	parallel: ->
		@_async = true
		return @_parallel_callback @_p_args, @_p_idx++
	
	# Allocate a slot in arguments for parallel execution
	group: ->
		@_async = true
		
		local_args = @_p_args[@_p_idx++] = []
		local_idx  = 0
		
		# Group-callback generator
		return => @_parallel_callback local_args, local_idx++
				
	# Generate parallel callback
	_parallel_callback: (arr, idx) ->
		@_p_count++
		
		# Init result value
		arr[idx] = undefined
		
		# Callback
		return (e, args...) =>
			return if @_done
			
			if e
				@error e
			else
				if args.length > 0
					# Extract from array if only one value
					arr[idx] = if args.length == 1 then args[0] else args
				@_parallel_done()
			
			return
	
	# One parallel call is done
	_parallel_done: ->
		@_p_done++
		@_parallel_flush()
		return
		
	# Unlock parallels completion
	_parallel_unlock: ->
		@_p_lock = false
		@_parallel_flush()
		return
		
	# Check parallels completion and continue to next step
	_parallel_flush: ->
		return if @_p_lock || @_done
		if @_p_count > 0 && @_p_count == @_p_done
			@_done = true
			@ctx.next undefined, @_p_args
		return

# Build a new flow
flowing = (args...) ->
	# If the only arg is a flow, return it
	if args.length == 1 && args[0]?._flow
		return args[0]
	
	steps = flowing.normalize args
	
	# The flow function
	flow = (args..., cb) ->
		if typeof cb != "function"
			args.push cb
			cb = -> #noop
		
		# Execution context
		ctx = new FlowingContext steps, args, cb
		
		# Bootstrap flow
		ctx.next undefined, args
		return
	
	flow._flow = true  # Tag as a flow for use as sub-flow
	flow.steps = steps # Expose steps
	
	flow.exec = (args...) -> flow args..., -> #noop
	
	return flow

# Create & execute a anonymous flow
flowing.exec = ->
	flow = flowing.apply null, arguments
	flow.exec()
	
# Normalize a deep input flow to a flat flow
flowing.normalize = (step) ->
	switch flowing_typeof step
		when "function"
			return [step]
		
		when "array"
			steps = []
			steps = steps.concat flowing.normalize s for s in step
			return steps
		
		when "object"
			# Objects are labeled steps
			steps = []
			
			for tag, fn of step
				if typeof fn != "function"
					throw new Error "Unable to label '#{fn}'"
				
				fn._label = tag
				steps.push fn
			
			return steps
		
		else
			throw new Error "Invalid flow step '#{step}'"

# Apply a tag to a step [private]
tag = (step, tag) ->
	switch flowing_typeof step
		when "function"
			step["_#{tag}"] = true
		
		when "object"
			# Extract step-label from the object
			labels = Object.keys step
			
			# Only one label is allowed with tags
			if labels.length > 1
				throw new Error "Flow branching is not allowed"
			
			# Extract the step function and label & tag it
			step = step[labels[0]]
			step._label = labels[0]
			step["_#{tag}"] = true
		
		else
			throw new Error "Unable to tag '#{step}'"
	
	return step

# Tag a step as error-only with an optional filter
flowing.error = (filter, step) ->
	if not step?
		# No filter given
		step = filter
		filter = undefined
	
	step = tag step, "error"
	step._filter = filter if typeof filter == "function"
	
	return step

# Tag a step as asynchronous
flowing.async = (step) -> tag step, "async"

# Tag a step as delayed
flowing.delayed = (step) -> tag step, "delay"

# Exports
flowing.version = "0.5.4"
module.exports  = flowing
